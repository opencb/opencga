package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOST                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOST";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_PORT                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.PORT";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS   = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES      = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE          = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BATCH_SIZE";

    @Override
    public VariantWriter getDBWriter(String dbName, ObjectMap params) {
        VariantSource source = params.get(SOURCE, VariantSource.class);
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
        try {
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(credentials);
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
        boolean includeEffect = params.getBoolean(INCLUDE_EFFECT);
        boolean includeStats = params.getBoolean(INCLUDE_STATS);
        VariantSource source = params.get(SOURCE, VariantSource.class);
        String dbName = params.getString(DB_NAME, null);
//        VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        int batchSize = Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE, "100"));

        //Reader
        VariantReader variantJsonReader;
        variantJsonReader = getVariantJsonReader(input, source);

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Writers
        VariantWriter variantDBWriter = this.getDBWriter(dbName, params);
        List<VariantWriter> writers = new ArrayList<>();
        writers.add(variantDBWriter);

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        //Runner
        VariantRunner vr = new VariantRunner(source, variantJsonReader, null, writers, taskList, batchSize);

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();
        vr.run();
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
