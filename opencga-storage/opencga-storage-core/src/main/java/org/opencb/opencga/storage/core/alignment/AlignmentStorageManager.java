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
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.json.AlignmentJsonDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 14/08/14.
 */
public abstract class AlignmentStorageManager implements StorageManager<DataWriter<AlignmentRegion>, AlignmentDBAdaptor> { //DataReader<AlignmentRegion>,


    public static final String MEAN_COVERAGE_SIZE_LIST = "meanCoverageSizeList";
    public static final String PLAIN = "plain";
    public static final String REGION_SIZE = "regionSize";
    public static final String STUDY = "study";
    public static final String FILE_ID = "fileId";
    public static final String INCLUDE_COVERAGE = "includeCoverage";
    public static final String ENCRYPT = "encrypt";
    public static final String COPY_FILE = "copy";
    public static final String DB_NAME = "dbName";

    protected Logger logger = LoggerFactory.getLogger(AlignmentStorageManager.class);


    @Override
    abstract public void transform(URI input, URI pedigree, URI output, ObjectMap params)
            throws IOException, FileFormatException;


    @Override
    public void load(URI input, URI credentials, ObjectMap params) throws IOException {
        String fileId = params.getString(FILE_ID, Paths.get(input).getFileName().toString().split("\\.")[0]);
        String dbName = params.getString(DB_NAME);

        AlignmentJsonDataReader alignmentDataReader = getAlignmentJsonDataReader(input);
        DataReader<AlignmentRegion> schemaReader = new AlignmentRegionDataReader(alignmentDataReader);
        DataWriter<AlignmentRegion> dbWriter = this.getDBWriter(credentials, dbName, fileId);

        Runner<AlignmentRegion> runner = new Runner<>(schemaReader, Arrays.asList(dbWriter), new LinkedList<Task<AlignmentRegion>>(), 1);

        logger.info("Loading alignments...");
        long start = System.currentTimeMillis();
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Alignments loaded!");

    }

    private AlignmentJsonDataReader getAlignmentJsonDataReader(URI input) throws IOException {
        if (!input.getScheme().equals("file")) {
            throw new IOException("URI is not a valid path");
        }

        String baseFileName = input.getPath();
        String alignmentFile = baseFileName;
        String headerFile;
        if(baseFileName.endsWith(".bam")){
            alignmentFile = baseFileName + (Paths.get(baseFileName + ".alignments.json").toFile().exists() ?
                    ".alignments.json" :
                    ".alignments.json.gz");
            headerFile = baseFileName + (Paths.get(baseFileName + ".header.json").toFile().exists() ?
                    ".header.json" :
                    ".header.json.gz");
        } else if(baseFileName.endsWith(".alignments.json")){
            headerFile = baseFileName.replaceFirst("alignments\\.json$", "header.json");
        } else if(baseFileName.endsWith(".alignments.json.gz")){
            headerFile = baseFileName.replaceFirst("alignments\\.json\\.gz$", "header.json.gz");
        } else {
            throw new IOException("Invalid input file : " + input.toString());
        }
        if (!Paths.get(alignmentFile).toFile().exists()) {
            throw new FileNotFoundException(alignmentFile);
        }
        if (!Paths.get(headerFile).toFile().exists()) {
            throw new FileNotFoundException(headerFile);
        }

        return new AlignmentJsonDataReader(alignmentFile, headerFile);
    }

}
