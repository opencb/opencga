package org.opencb.opencga.storage.core.alignment;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataReader;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.alignment.json.AlignmentJsonDataReader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

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
    abstract public DataWriter<AlignmentRegion> getDBWriter(Path credentials, String fileId);

    @Override
    abstract public AlignmentQueryBuilder getDBAdaptor(Path credentials);

    @Override
    abstract public void transform(Path input, Path pedigree, Path output, Map<String, Object> params) throws IOException, FileFormatException;

    @Override
    public void preLoad(Path input, Path output, Map<String, Object> params) throws IOException {

    }

    @Override
    public void load(Path input, Path credentials, Map<String, Object> params) throws IOException {

        String fileId = params.containsKey(FILE_ID)? params.get(FILE_ID).toString(): input.getFileName().toString().split("\\.")[0];

        DataReader<AlignmentRegion> schemaReader = this.getDBSchemaReader(input);
        DataWriter<AlignmentRegion> dbWriter = this.getDBWriter(credentials, fileId);

        Runner runner = new Runner(schemaReader, Arrays.asList(dbWriter), new LinkedList<Task>());

        System.out.println("Loading alignments...");
        runner.run();
        System.out.println("Alignments loaded!");

    }
}
