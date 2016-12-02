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
        CoverageDBManager coverageDBManager = new CoverageDBManager();
        SAMFileHeader fileHeader = BamUtils.getFileHeader(path);
        Path coverageDBPath = coverageDBManager.create(path);

        Path coveragePath = workspace.toAbsolutePath().resolve(path.getFileName()
                + LocalAlignmentGlobals.COVERAGE_WIG_SUFFIX);

        FileOutputStream fos = new FileOutputStream(coveragePath.toString());
        OutputStream os = new BufferedOutputStream(fos);
        PrintStream ps = new PrintStream(os);

        AlignmentOptions options = new AlignmentOptions();
        Iterator<SAMSequenceRecord> iterator = fileHeader.getSequenceDictionary().getSequences().iterator();
        long start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            SAMSequenceRecord next = iterator.next();
            for (int i = 0; i < next.getSequenceLength(); i += LocalAlignmentGlobals.COVERAGE_REGION_SIZE) {
                Region region = new Region(next.getSequenceName(), i + 1,
                        Math.min(i + LocalAlignmentGlobals.COVERAGE_REGION_SIZE, next.getSequenceLength()));
                RegionCoverage regionCoverage = bamManager.coverage(region, null, options);

                // write coverage to wigfile
                BamUtils.printWigFileCoverage(regionCoverage, LocalAlignmentGlobals.DEFAULT_WINDOW_SIZE,
                        regionCoverage.getStart() == 1, ps);
            }
        }

        // closing stuff
        ps.close();
        os.close();
        fos.close();
        System.err.println("LocalAlignmentStorageETL: Coverage file creation (window size " + LocalAlignmentGlobals.DEFAULT_WINDOW_SIZE
                + "), in " + ((System.currentTimeMillis() - start) / 1000.0f) + " s.");

        // save file to db
        coverageDBManager.loadWigFile(coveragePath);

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
