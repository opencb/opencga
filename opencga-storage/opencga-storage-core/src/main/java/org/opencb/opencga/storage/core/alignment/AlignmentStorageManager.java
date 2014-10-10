package org.opencb.opencga.storage.core.alignment;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.alignment.json.AlignmentJsonDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by jacobo on 14/08/14.
 */
public abstract class AlignmentStorageManager implements StorageManager<DataReader<AlignmentRegion>, DataWriter<AlignmentRegion>, AlignmentQueryBuilder> {


    public static final String MEAN_COVERAGE_SIZE_LIST = "meanCoverageSizeList";
    public static final String PLAIN = "plain";
    public static final String REGION_SIZE = "regionSize";
    public static final String STUDY = "study";
    public static final String FILE_ID = "fileId";
    public static final String INCLUDE_COVERAGE = "includeCoverage";
    public static final String ENCRYPT = "encrypt";
    public static final String COPY_FILE = "copy";
    public static final String DB_NAME = "dbName";

    protected static Logger logger = LoggerFactory.getLogger(AlignmentStorageManager.class);


    @Override
    public DataWriter<AlignmentRegion> getDBSchemaWriter(Path output) {
        return null;
    }

    @Override
    public DataReader<AlignmentRegion> getDBSchemaReader(Path input) {
        String headerFile = input.toString()
                .replaceFirst("alignment\\.json$", "header.json")
                .replaceFirst("alignment\\.json\\.gz$", "header.json.gz");
        Path header = Paths.get(headerFile);
        return new AlignmentRegionDataReader(new AlignmentJsonDataReader(input.toString(),header.toString()));
    }

    @Override
    abstract public DataWriter<AlignmentRegion> getDBWriter(Path credentials, String dbName, String fileId);

    @Override
    abstract public AlignmentQueryBuilder getDBAdaptor(String dbName);

    @Override
    abstract public void transform(Path input, Path pedigree, Path output, ObjectMap params) throws IOException, FileFormatException;

    @Override
    public void preLoad(Path input, Path output, ObjectMap params) throws IOException {

    }

    @Override
    public void load(Path input, Path credentials, ObjectMap params) throws IOException {

        String fileId = params.getString(FILE_ID, input.getFileName().toString().split("\\.")[0]);
        String dbName = params.getString(DB_NAME);

        DataReader<AlignmentRegion> schemaReader = this.getDBSchemaReader(input);
        DataWriter<AlignmentRegion> dbWriter = this.getDBWriter(credentials, dbName, fileId);

        Runner<AlignmentRegion> runner = new Runner<>(schemaReader, Arrays.asList(dbWriter), new LinkedList<Task<AlignmentRegion>>(), 1);

        logger.info("Loading alignments...");
        long start = System.currentTimeMillis();
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Alignments loaded!");

    }
}
