package org.opencb.opencga.storage.core.alignment.local;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.biodata.tools.commons.ChunkFrequencyManager;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by pfurio on 31/10/16.
 */
public class LocalAlignmentStorageETL implements StorageETL {

    private static final String COVERAGE_SUFFIX = ".coverage";
    private static final String COVERAGE_DATABASE_NAME = "coverage.db";

    private static final int MINOR_CHUNK_SIZE = 1000;

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
            ObjectWriter objectWriter = objectMapper.typedWriter(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), stats);
        }

        // 3. Calculate coverage and store in SQLite
        SAMFileHeader fileHeader = BamUtils.getFileHeader(path);
//        long start = System.currentTimeMillis();

        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
        ChunkFrequencyManager chunkFrequencyManager = new ChunkFrequencyManager(coverageDBPath);
        List<String> chromosomeNames = new ArrayList<>();
        List<Integer> chromosomeLengths = new ArrayList<>();
        fileHeader.getSequenceDictionary().getSequences().forEach(
                seq -> {
                    chromosomeNames.add(seq.getSequenceName());
                    chromosomeLengths.add(seq.getSequenceLength());
                });
        chunkFrequencyManager.init(chromosomeNames, chromosomeLengths);
//        System.out.println("SQLite database initialization, in " + ((System.currentTimeMillis() - start) / 1000.0f)
//                + " s.");

        Path coveragePath = workspace.toAbsolutePath().resolve(path.getFileName() + COVERAGE_SUFFIX);

        AlignmentOptions options = new AlignmentOptions();
        options.setContained(false);

        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
        PrintWriter writer = new PrintWriter(coveragePath.toFile());
        StringBuilder line;
//        start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += MINOR_CHUNK_SIZE) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + MINOR_CHUNK_SIZE, next.getSequenceLength()));
                RegionCoverage regionCoverage = bamManager.coverage(region, options, null);
                int meanDepth = Math.min(regionCoverage.meanCoverage(), 255);

                // File columns: chunk   chromosome start   end coverage
                // chunk format: chrom_id_suffix, where:
                //      id: int value starting at 0
                //      suffix: chunkSize + k
                // eg. 3_4_1k

                line = new StringBuilder();
                line.append(region.getChromosome()).append("_");
                line.append(i / MINOR_CHUNK_SIZE).append("_").append(MINOR_CHUNK_SIZE / 1000).append("k");
                line.append("\t").append(region.getChromosome());
                line.append("\t").append(region.getStart());
                line.append("\t").append(region.getEnd());
                line.append("\t").append(meanDepth);
                writer.println(line.toString());
            }
        }
        writer.close();
//        System.out.println("Mean coverage file creation, in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");

        // save file to db
//        start = System.currentTimeMillis();

        chunkFrequencyManager.load(coveragePath, path);
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
