package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.data.source.Source;
import org.opencb.opencga.storage.core.ThreadRunner;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS       = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.WRITE_THREADS";

    //StorageEngine specific params
    public static final String WRITE_MONGO_THREADS = "writeMongoThreads";
    public static final String BULK_SIZE = "bulkSize";
    public static final String INCLUDE_SRC = "includeSrc";

    @Override
    public VariantMongoDBWriter getDBWriter(String dbName, ObjectMap params) {
        VariantSource source = params.get(VARIANT_SOURCE, VariantSource.class);
        Properties credentialsProperties = new Properties(properties);

        MongoCredentials credentials = getMongoCredentials(dbName);
        String variantsCollection = credentialsProperties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS, "variants");
        String filesCollection = credentialsProperties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES, "files");
//        String variantsCollection = credentialsProperties.getProperty("collection_variants", "variants");
//        String filesCollection = credentialsProperties.getProperty("collection_files", "files");
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

        boolean includeSamples = params.getBoolean(INCLUDE_SAMPLES);
        boolean includeEffect  = params.getBoolean(INCLUDE_EFFECT);
        boolean includeStats   = params.getBoolean(INCLUDE_STATS);
        boolean includeSrc     = params.getBoolean(INCLUDE_SRC);
        VariantSource source = new VariantSource(inputUri.getPath(), "", "", "");       //Create a new VariantSource. This object will be filled at the VariantJsonReader in the pre()
        params.put(VARIANT_SOURCE, source);
        String dbName = params.getString(DB_NAME, null);

        int batchSize = params.getInt(BATCH_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE, "100")));
        int bulkSize = params.getInt(BULK_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BULK_SIZE, "" + batchSize)));
        int numWriters = params.getInt(WRITE_MONGO_THREADS, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS, "8")));

        //Reader
        VariantReader variantJsonReader;
        variantJsonReader = getVariantJsonReader(input, source);

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Writers
        List<VariantWriter> writers = new ArrayList<>();
        for (int i = 0; i < numWriters; i++) {
            VariantMongoDBWriter variantDBWriter = this.getDBWriter(dbName, params);
            variantDBWriter.setBulkSize(bulkSize);
            variantDBWriter.includeSrc(includeSrc);
            writers.add(variantDBWriter);
        }

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        //Runner
//        VariantRunner vr = new VariantRunner(source, variantJsonReader, null, writers, taskList, batchSize);
        Runner<Variant> r = new ThreadRunner<>(
                variantJsonReader,
                Collections.<List<? extends DataWriter<Variant>>>singleton(writers),
                Collections.<Task<Variant>>emptyList(),
                batchSize,
                new Variant());

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();
//        vr.run();
        r.run();
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
