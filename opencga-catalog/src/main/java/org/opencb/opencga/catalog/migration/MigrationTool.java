package org.opencb.opencga.catalog.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public abstract class MigrationTool {

    protected Configuration configuration;
    protected CatalogManager catalogManager;
    protected MongoDBAdaptorFactory dbAdaptorFactory;
    protected MigrationRun migrationRun;
    protected Path appHome;
    protected String token;

    protected ObjectMap params;

    protected final Logger logger;
    private final Logger privateLogger;
    private GenericDocumentComplexConverter<Object> converter;
    private final int BATCH_SIZE;

    public MigrationTool() {
        this(1000);
    }

    public MigrationTool(int batchSize) {
        this.logger = LoggerFactory.getLogger(this.getClass());
        // Internal logger
        this.privateLogger = LoggerFactory.getLogger(MigrationTool.class);
        this.BATCH_SIZE = batchSize;
        this.converter = new GenericDocumentComplexConverter<>(Object.class, JacksonUtils.getDefaultObjectMapper());
    }

    public final Migration getAnnotation() {
        return getClass().getAnnotation(Migration.class);
    }

    public final String getId() {
        return getAnnotation().id();
    }

    public final void setup(Configuration configuration, CatalogManager catalogManager, MongoDBAdaptorFactory dbAdaptorFactory,
                            MigrationRun migrationRun, Path appHome, ObjectMap params, String token) {
        this.configuration = configuration;
        this.catalogManager = catalogManager;
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.migrationRun = migrationRun;
        this.appHome = appHome;
        this.params = params;
        this.token = token;
    }

    public final void execute() throws MigrationException {
        try {
            run();
        } catch (MigrationException e) {
            throw e;
        } catch (Exception e) {
            throw new MigrationException("Error running migration '" + getId() + "' : " + e.getMessage(), e);
        }
    }

    protected abstract void run() throws Exception;

    protected final MigrationRun getMigrationRun() {
        return migrationRun;
    }

    protected final StorageConfiguration readStorageConfiguration() throws MigrationException {
        try (FileInputStream is = new FileInputStream(appHome.resolve("conf").resolve("storage-configuration.yml").toFile())) {
            return StorageConfiguration.load(is);
        } catch (IOException e) {
            throw new MigrationException("Error reading \"storage-configuration.yml\"", e);
        }
    }

    protected final void runJavascript(Path file) throws MigrationException {
        String authentication = "";
        if (StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getUser())
                && StringUtils.isNotEmpty(configuration.getCatalog().getDatabase().getPassword())) {
            authentication = "-u " + configuration.getCatalog().getDatabase().getUser() + " -p "
                    + configuration.getCatalog().getDatabase().getPassword() + " --authenticationDatabase "
                    + configuration.getCatalog().getDatabase().getOptions().getOrDefault("authenticationDatabase", "admin") + " ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_ENABLED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.SSL_ENABLED))) {
            authentication += "--ssl ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_INVALID_CERTIFICATES_ALLOWED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions()
                .get(MongoDBConfiguration.SSL_INVALID_CERTIFICATES_ALLOWED))) {
            authentication += "--sslAllowInvalidCertificates ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null
                && configuration.getCatalog().getDatabase().getOptions().containsKey(MongoDBConfiguration.SSL_INVALID_HOSTNAME_ALLOWED)
                && Boolean.parseBoolean(configuration.getCatalog().getDatabase().getOptions()
                .get(MongoDBConfiguration.SSL_INVALID_HOSTNAME_ALLOWED))) {
            authentication += "--sslAllowInvalidHostnames ";
        }
        if (configuration.getCatalog().getDatabase().getOptions() != null && StringUtils.isNotEmpty(
                configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.AUTHENTICATION_MECHANISM))) {
            authentication += "--authenticationMechanism "
                    + configuration.getCatalog().getDatabase().getOptions().get(MongoDBConfiguration.AUTHENTICATION_MECHANISM) + " ";
        }

        String catalogCli = "mongo " + authentication
                + StringUtils.join(configuration.getCatalog().getDatabase().getHosts(), ",") + "/"
                + catalogManager.getCatalogDatabase() + " " + file.getFileName();

        privateLogger.info("Running Javascript cli {} from {}", catalogCli, file.getParent());
        ProcessBuilder processBuilder = new ProcessBuilder(catalogCli.split(" "));
        processBuilder.directory(file.getParent().toFile());
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                privateLogger.info(line);
            }
            p.waitFor();
            input.close();
        } catch (IOException | InterruptedException e) {
            throw new MigrationException("Error executing cli: " + e.getMessage(), e);
        }

        if (p.exitValue() == 0) {
            privateLogger.info("Finished Javascript catalog migration");
        } else {
            throw new MigrationException("Error with Javascript catalog migrating!");
        }
    }

    @FunctionalInterface
    protected interface MigrateCollectionFunc {
        void accept(Document document, List<WriteModel<Document>> bulk);
    }

    @FunctionalInterface
    protected interface QueryCollectionFunc {
        void accept(Document document);
    }

    protected final void migrateCollection(String collection, Bson query, Bson projection,
                                           MigrateCollectionFunc migrateFunc) {
        migrateCollection(collection, collection, query, projection, migrateFunc);
    }

    protected final void migrateCollection(String inputCollection, String outputCollection, Bson query, Bson projection,
                                           MigrateCollectionFunc migrateFunc) {
        migrateCollection(getMongoCollection(inputCollection), getMongoCollection(outputCollection), query, projection, migrateFunc);
    }

    protected final void migrateCollection(MongoCollection<Document> inputCollection, MongoCollection<Document> outputCollection,
                                           Bson query, Bson projection,
                                           MigrateCollectionFunc migrateFunc) {
        int count = 0;
        List<WriteModel<Document>> list = new ArrayList<>(BATCH_SIZE);

        ProgressLogger progressLogger = new ProgressLogger("Execute bulk update").setBatchSize(BATCH_SIZE);
        try (MongoCursor<Document> it = inputCollection.find(query).projection(projection).cursor()) {
            while (it.hasNext()) {
                Document document = it.next();
                migrateFunc.accept(document, list);

                if (list.size() >= BATCH_SIZE) {
                    count += list.size();
                    progressLogger.increment(list.size());
                    outputCollection.bulkWrite(list);
                    list.clear();
                }
            }
            if (!list.isEmpty()) {
                count += list.size();
                progressLogger.increment(list.size());
                outputCollection.bulkWrite(list);
                list.clear();
            }
        }
        if (count == 0) {
            privateLogger.info("Nothing to do!");
        } else {
            privateLogger.info("Updated {} documents from collection {}", count, outputCollection.getNamespace().getFullName());
        }
    }

    protected final void queryMongo(String inputCollectionStr, Bson query, Bson projection, QueryCollectionFunc queryCollectionFunc) {
        MongoCollection<Document> inputCollection = getMongoCollection(inputCollectionStr);

        try (MongoCursor<Document> it = inputCollection.find(query).projection(projection).cursor()) {
            while (it.hasNext()) {
                Document document = it.next();
                queryCollectionFunc.accept(document);
            }
        }
    }

    protected final MongoCollection<Document> getMongoCollection(String collectionName) {
        return dbAdaptorFactory.getMongoDataStore().getDb().getCollection(collectionName);
    }

    protected <T> Document convertToDocument(T value) {
        return converter.convertToStorageType(value);
    }

    protected <T> List<Document> convertToDocument(List<T> values) {
        List<Document> documentList = new ArrayList<>(values.size());
        for (Object value : values) {
            documentList.add(convertToDocument(value));
        }
        return documentList;
    }

}
