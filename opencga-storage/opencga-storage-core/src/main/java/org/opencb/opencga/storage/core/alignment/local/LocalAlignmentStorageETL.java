package org.opencb.opencga.storage.core.alignment.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by pfurio on 31/10/16.
 */
public class LocalAlignmentStorageETL implements StorageETL {

    public LocalAlignmentStorageETL() {
        super();
    }

    @Override
    public URI extract(URI input, URI ouput) throws StorageManagerException {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws IOException, FileFormatException, StorageManagerException {
        // Check if a BAM file is passed and it is sorted.
        // Only binaries and sorted BAM files are accepted at this point.
        Path inputPath = Paths.get(input.getRawPath());
        BamUtils.checkBamOrCramFile(new FileInputStream(inputPath.toFile()), inputPath.getFileName().toString(), true);
        return input;
    }

    @Override
    public URI transform(URI input, URI pedigree, URI output) throws Exception {
        Path path = Paths.get(input.getRawPath());
        FileUtils.checkFile(path);

        Path workspace = Paths.get(output.getRawPath());
        FileUtils.checkDirectory(workspace);

        // 1. Check if the bai does not exist and create it
        BamManager bamManager = new BamManager(path);
        if (!path.getParent().resolve(path.getFileName().toString() + ".bai").toFile().exists()) {
            bamManager.createIndex();
        }

        // 2. Calculate stats and store in a file
        Path statsPath = workspace.resolve(path.getFileName() + ".stats");
        if (!statsPath.toFile().exists()) {
            AlignmentGlobalStats stats = bamManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.writerFor(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), stats);
        }

        // 3. Calculate coverage and store in SQLite
        SAMFileHeader fileHeader = BamUtils.getFileHeader(path);
//        long start = System.currentTimeMillis();
        LocalAlignmentUtils.initDatabase(fileHeader.getSequenceDictionary().getSequences(), workspace);
//        System.out.println("SQLite database initialization, in " + ((System.currentTimeMillis() - start) / 1000.0f)
//                + " s.");

        Path coveragePath = workspace.toAbsolutePath().resolve(path.getFileName()
                + LocalAlignmentUtils.COVERAGE_WIG_SUFFIX);

        FileOutputStream fos = new FileOutputStream(coveragePath.toString());
        OutputStream os = new BufferedOutputStream(fos);
        PrintStream ps = new PrintStream(os);

        AlignmentOptions options = new AlignmentOptions();
        options.setContained(false);


        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
//        PrintWriter writer = new PrintWriter(coveragePath.toFile());
//        StringBuilder line;
//        start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += LocalAlignmentUtils.COVERAGE_REGION_SIZE) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + LocalAlignmentUtils.COVERAGE_REGION_SIZE, next.getSequenceLength()));
                RegionCoverage regionCoverage = bamManager.coverage(region, null, options);

                // write coverage to wigfile
                BamUtils.printWigFileCoverage(regionCoverage, LocalAlignmentUtils.CHUNK_SIZE,
                        regionCoverage.getStart() == 1, ps);

//                int meanDepth = Math.min(regionCoverage.meanCoverage(), 255);
//
//                // File columns: chunk   chromosome start   end coverage
//                // chunk format: chrom_id_suffix, where:
//                //      id: int value starting at 0
//                //      suffix: chunkSize + k
//                // eg. 3_4_1k
//
//                line = new StringBuilder();
//                line.append(region.getChromosome()).append("_");
//                line.append(i / MINOR_CHUNK_SIZE).append("_").append(MINOR_CHUNK_SIZE / 1000).append("k");
//                line.append("\t").append(region.getChromosome());
//                line.append("\t").append(region.getStart());
//                line.append("\t").append(region.getEnd());
//                line.append("\t").append(meanDepth);
//                writer.println(line.toString());
            }
        }

        // closing stuff
        ps.close();
        os.close();
        fos.close();

//        writer.close();
//        System.out.println("Mean coverage file creation, in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");

        // save file to db
//        start = System.currentTimeMillis();
        LocalAlignmentUtils.insertCoverageDBFromWig(path, workspace);
//        System.out.println("SQLite database population, in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");

        return input;
    }

    @Override
    public URI postTransform(URI input) throws Exception {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }
}
