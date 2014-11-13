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
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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

    @Override
    public VariantWriter getDBWriter(String dbName, ObjectMap params) {
        VariantSource source = params.get(SOURCE, VariantSource.class);
        Properties credentialsProperties = new Properties(properties);

        MongoCredentials credentials = getMongoCredentials(dbName);
        String variantsCollection = credentialsProperties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS", "variants");
        String filesCollection = credentialsProperties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES", "files");
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
        String host = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOST");
        int port = Integer.parseInt(properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PORT", "27017"));
        if(dbName == null || dbName.isEmpty()) {
            dbName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME");
        }
        String user = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER", null);
        String pass = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS", null);

        try {
            return new MongoCredentials(host, port, dbName, user, pass);
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void preLoad(URI input, URI output, ObjectMap params) throws IOException {

    }

    @Override
    public void load(URI inputUri, ObjectMap params) throws IOException {
        // input: getDBSchemaReader
        // output: getDBWriter()

        Path input = Paths.get(inputUri.getPath());

        boolean includeSamples = params.getBoolean(INCLUDE_SAMPLES);
        boolean includeEffect = params.getBoolean(INCLUDE_EFFECT);
        boolean includeStats = params.getBoolean(INCLUDE_STATS);
        VariantSource source = params.get(SOURCE, VariantSource.class);
        String dbName = params.getString(DB_NAME, null);
//        VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

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
        VariantRunner vr = new VariantRunner(source, variantJsonReader, null, writers, taskList);

        logger.info("Loading variants...");
        long start = System.currentTimeMillis();
        vr.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");
    }

    @Override
    public void postLoad(URI input, URI output, ObjectMap params) throws IOException {

    }

}
