package org.opencb.opencga.storage.mongodb.variant;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;

import org.opencb.opencga.storage.core.runner.SimpleThreadRunner;
import org.opencb.opencga.storage.core.runner.ThreadRunner;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    //StorageEngine specific Properties
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOST                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOST";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_PORT                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.PORT";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS   = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES      = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE          = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BATCH_SIZE";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BULK_SIZE           = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BULK_SIZE";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_THREADS             = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.THREADS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS       = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.WRITE_THREADS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DEFAULT_GENOTYPE         = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.DEFAULT_GENOTYPE";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_COMPRESS_GENEOTYPES      = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.COMPRESS_GENOTYPES";

    //StorageEngine specific params
    public static final String WRITE_MONGO_THREADS = "writeMongoThreads";
    public static final String LOAD_THREADS = "loadThreads";
    public static final String BULK_SIZE = "bulkSize";
    public static final String INCLUDE_SRC = "includeSrc";
    public static final String DEFAULT_GENOTYPE = "defaultGenotype";

    protected static Logger logger = LoggerFactory.getLogger(MongoDBVariantStorageManager.class);

    @Override
    public VariantMongoDBWriter getDBWriter(String dbName, ObjectMap params) {
        VariantSource source = params.get(VARIANT_SOURCE, VariantSource.class);
        Properties credentialsProperties = new Properties(properties);

        MongoCredentials credentials = getMongoCredentials(dbName);
        String variantsCollection = credentialsProperties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS, "variants");
        String filesCollection = credentialsProperties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES, "files");
//        String variantsCollection = credentialsProperties.getProperty("collection_variants", "variants");
//        String filesCollection = credentialsProperties.getProperty("collection_files", "files");
        logger.debug("getting DBWriter to db: {}", credentials.getMongoDbName());
        return new VariantMongoDBWriter(source, credentials, variantsCollection, filesCollection);
    }

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName, ObjectMap params) {
        MongoCredentials credentials = getMongoCredentials(dbName);
        VariantMongoDBAdaptor variantMongoDBAdaptor;

        String variantsCollection = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS, "variants");
        String filesCollection = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES, "files");
        try {
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(credentials, variantsCollection, filesCollection);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }

        logger.debug("getting DBAdaptor to db: {}", credentials.getMongoDbName());
        return variantMongoDBAdaptor;
    }

    private MongoCredentials getMongoCredentials(String dbName) {
        String host = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOST, "localhost");
        int port = Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_PORT, "27017"));
        if(dbName == null || dbName.isEmpty()) {
            dbName = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME, "variants");
        }
        String user = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER, null);
        String pass = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS, null);

        try {
            return new MongoCredentials(host, port, dbName, user, pass);
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public URI preLoad(URI input, URI output, ObjectMap params) throws IOException {
        return input;
    }

    @Override
    public URI load(URI inputUri, ObjectMap params) throws IOException {
        // input: getDBSchemaReader
        // output: getDBWriter()

        Path input = Paths.get(inputUri.getPath());

        boolean includeSamples = params.getBoolean(INCLUDE_SAMPLES, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES, "false")));
//        boolean includeEffect = params.getBoolean(INCLUDE_EFFECT, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_EFFECT, "false")));
        boolean includeStats = params.getBoolean(INCLUDE_STATS, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_STATS, "false")));
        boolean includeSrc = params.getBoolean(INCLUDE_SRC, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SRC, "false")));

        String defaultGenotype = params.getString(DEFAULT_GENOTYPE, properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DEFAULT_GENOTYPE, ""));
        boolean compressSamples = params.getBoolean(COMPRESS_GENOTYPES, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_COMPRESS_GENEOTYPES, "false")));

        VariantSource source = new VariantSource(inputUri.getPath(), "", "", "");       //Create a new VariantSource. This object will be filled at the VariantJsonReader in the pre()
        params.put(VARIANT_SOURCE, source);
        String dbName = params.getString(DB_NAME, null);

        int batchSize = params.getInt(BATCH_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE, "100")));
        int bulkSize = params.getInt(BULK_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BULK_SIZE, "" + batchSize)));
        int numWriters = params.getInt(WRITE_MONGO_THREADS, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS, "1")));
        int loadThreads = params.getInt(LOAD_THREADS, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_THREADS, "1")));
//        Map<String, Integer> samplesIds = (Map) params.getMap("sampleIds");
        Map<String, Integer> samplesIds = new HashMap<>();
        for (String sampleId : params.getString("sampleIds").split(",")) {
            String[] split = sampleId.split(":");
            if (split.length != 2) {

            } else {
                samplesIds.put(split[0], Integer.parseInt(split[1]));
            }
        }

        if (loadThreads == 1) {
            numWriters = 1;     //Only 1 writer for the single thread execution
        }

        //Reader
        VariantReader variantJsonReader;
        variantJsonReader = getVariantJsonReader(input, source);

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Writers
        List<VariantWriter> writers = new LinkedList<>();
        List<DataWriter> writerList = new LinkedList<>();
        for (int i = 0; i < numWriters; i++) {
            VariantMongoDBWriter variantDBWriter = this.getDBWriter(dbName, params);
            variantDBWriter.setBulkSize(bulkSize);
            variantDBWriter.includeSrc(includeSrc);
            variantDBWriter.includeSamples(includeSamples);
            variantDBWriter.includeStats(includeStats);
            variantDBWriter.setCompressDefaultGenotype(compressSamples);
            variantDBWriter.setDefaultGenotype(defaultGenotype);
            variantDBWriter.setSamplesIds(samplesIds);
            writerList.add(variantDBWriter);
            writers.add(variantDBWriter);
        }



        logger.info("Loading variants...");
        long start = System.currentTimeMillis();

        //Runner
        if (loadThreads == 1) {
            logger.info("Single thread load...");
            VariantRunner vr = new VariantRunner(source, variantJsonReader, null, writers, taskList, batchSize);
            vr.run();
        } else {
            logger.info("Multi thread load...");
//            ThreadRunner runner = new ThreadRunner(Executors.newFixedThreadPool(loadThreads), batchSize);
//            ThreadRunner.ReadNode<Variant> variantReadNode = runner.newReaderNode(variantJsonReader, 1);
//            ThreadRunner.WriterNode<Variant> variantWriterNode = runner.newWriterNode(writerList);
//
//            variantReadNode.append(variantWriterNode);
//            runner.run();

            SimpleThreadRunner threadRunner = new SimpleThreadRunner(
                    variantJsonReader,
                    Collections.<Task>emptyList(),
                    writerList,
                    batchSize,
                    loadThreads*2,
                    0);
            threadRunner.run();

        }

        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");

        return inputUri; //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    @Override
    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException {
        return super.postLoad(input, output, params);
    }

}
