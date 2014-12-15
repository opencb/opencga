package org.opencb.opencga.storage.mongodb.alignment;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataReader;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Date 15/08/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */
public class MongoDBAlignmentStorageManager extends AlignmentStorageManager {

    public static final String MONGO_DB_NAME = "opencga";
    public static final String OPENCGA_STORAGE_SEQUENCE_DBADAPTOR      = "OPENCGA.STORAGE.SEQUENCE.DB.ROOTDIR";
    public static final String OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_NAME = "OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.NAME";
    public static final String OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_USER = "OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.USER";
    public static final String OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_PASS = "OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.PASS";
    public static final String OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_HOST = "OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.HOST";
    public static final String OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_PORT = "OPENCGA.STORAGE.MONGO.ALIGNMENT.DB.PORT";

    //private static Path indexerManagerScript = Paths.get(Config.getGcsaHome(), Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"), "indexer", "indexerManager.py");
    protected static Logger logger = LoggerFactory.getLogger(MongoDBAlignmentStorageManager.class);

    public MongoDBAlignmentStorageManager(Path propertiesPath) {
        this();
        addConfigUri(URI.create(propertiesPath.toString()));
    }

    public MongoDBAlignmentStorageManager() {
        super();
    }

    @Override
    public CoverageMongoDBWriter getDBWriter(String dbName, ObjectMap params) {
        String fileId = params.getString(FILE_ID);
        return new CoverageMongoDBWriter(getMongoCredentials(dbName), fileId);
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName, ObjectMap params) {
        SequenceDBAdaptor adaptor;
        if (dbName == null || dbName.isEmpty()) {
            dbName = properties.getProperty(OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_NAME, MONGO_DB_NAME);
            logger.info("Using default dbName in MongoDBAlignmentStorageManager.getDBAdaptor()");
        }
        Path path = Paths.get(properties.getProperty(OPENCGA_STORAGE_SEQUENCE_DBADAPTOR, ""));
        if (path == null || path.toString() == null || path.toString().isEmpty() || !path.toFile().exists()) {
            adaptor = new CellBaseSequenceDBAdaptor();
        } else {
            if(path.toString().endsWith("sqlite.db")) {
                adaptor = new SqliteSequenceDBAdaptor(path);
            } else {
                adaptor = new CellBaseSequenceDBAdaptor(path);
            }
        }

        return new IndexedAlignmentDBAdaptor(adaptor, getMongoCredentials(dbName));
    }

    private MongoCredentials getMongoCredentials(String mongoDbName){
        try {   //TODO: Use user and password
            String mongoUser = properties.getProperty(OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_USER, null);
            String mongoPassword = properties.getProperty(OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_PASS, null);
            return new MongoCredentials(
                    properties.getProperty(OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_HOST, "localhost"),
                    Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGO_ALIGNMENT_DB_PORT, "27017")),
                    mongoDbName,
                    null,
                    null
            );
            //this.mongoCredentials = new MongoCredentials(properties);
        } catch (IllegalOpenCGACredentialsException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }






    @Override
    public URI transform(URI inputUri, URI pedigree, URI outputUri, ObjectMap params) throws IOException, FileFormatException {
        params.put(WRITE_ALIGNMENTS, false);
        params.put(CREATE_BAI, true);
        params.put(INCLUDE_COVERAGE, true);
        return super.transform(inputUri, pedigree, outputUri, params);
    }


    @Override
    public URI preLoad(URI input, URI output, ObjectMap params) throws IOException {
        return input;
    }

    @Override
    public URI load(URI inputUri, ObjectMap params) throws IOException {
        checkUri(inputUri, "input uri");
        Path input = Paths.get(inputUri.getPath());

        String fileId = params.getString(FILE_ID, input.getFileName().toString().split("\\.")[0]);
        String dbName = params.getString(DB_NAME);


        //Reader
        AlignmentCoverageJsonDataReader alignmentDataReader = getAlignmentCoverageJsonDataReader(input);
        alignmentDataReader.setReadRegionCoverage(false);   //Only load mean coverage

        //Writer
        CoverageMongoDBWriter dbWriter = this.getDBWriter(dbName, new ObjectMap(FILE_ID, fileId));

        //Runner
        Runner<AlignmentRegion> runner = new Runner<>(alignmentDataReader, Arrays.asList(dbWriter), new LinkedList<Task<AlignmentRegion>>(), 1);

        logger.info("Loading coverage...");
        long start = System.currentTimeMillis();
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Alignments loaded!");

        return inputUri;    //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    @Override
    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException {
        return input;
    }

}
