package org.opencb.opencga.catalog.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
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
import java.util.Collections;
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
    private int batchSize;
    private StorageConfiguration storageConfiguration;

    public MigrationTool() {
        this(500);
    }

    public MigrationTool(int batchSize) {
        this.logger = LoggerFactory.getLogger(this.getClass());
        // Internal logger
        this.privateLogger = LoggerFactory.getLogger(MigrationTool.class);
        this.batchSize = batchSize;
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
        if (storageConfiguration == null) {
            try (FileInputStream is = new FileInputStream(appHome.resolve("conf").resolve("storage-configuration.yml").toFile())) {
                storageConfiguration = StorageConfiguration.load(is);
            } catch (IOException e) {
                throw new MigrationException("Error reading \"storage-configuration.yml\"", e);
            }
        }
        return storageConfiguration;
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

    protected final void migrateCollection(String collection, Bson query, Bson projection, MigrateCollectionFunc migrateFunc) {
        migrateCollection(collection, collection, query, projection, migrateFunc);
    }

    protected final void migrateCollection(List<String> collections, Bson query, Bson projection, MigrateCollectionFunc migrateFunc) {
        for (String collection : collections) {
            privateLogger.info("Starting migration in {}", collection);
            migrateCollection(collection, collection, query, projection, migrateFunc);
        }
    }

    protected final void migrateCollection(String inputCollection, String outputCollection, Bson query, Bson projection,
                                           MigrateCollectionFunc migrateFunc) {
        migrateCollection(getMongoCollection(inputCollection), getMongoCollection(outputCollection), query, projection, migrateFunc);
    }

    protected final void migrateCollection(MongoCollection<Document> inputCollection, MongoCollection<Document> outputCollection,
                                           Bson query, Bson projection, MigrateCollectionFunc migrateFunc) {
        int count = 0;
        List<WriteModel<Document>> list = new ArrayList<>(batchSize);

        ProgressLogger progressLogger = new ProgressLogger("Execute bulk update").setBatchSize(batchSize);
        try (MongoCursor<Document> it = inputCollection
                .find(query)
                .batchSize(batchSize)
                .noCursorTimeout(true)
                .projection(projection)
                .cursor()) {
            while (it.hasNext()) {
                Document document = it.next();
                migrateFunc.accept(document, list);

                if (list.size() >= batchSize) {
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

    protected final void createIndex(String collection, Document index) {
        createIndex(getMongoCollection(collection), index, new IndexOptions().background(true));
    }

    protected final void createIndex(List<String> collections, Document index) {
        createIndexes(collections, Collections.singletonList(index));
    }

    protected final void createIndexes(List<String> collections, List<Document> indexes) {
        for (String collection : collections) {
            for (Document index : indexes) {
                createIndex(getMongoCollection(collection), index, new IndexOptions().background(true));
            }
        }
    }

    protected final void createIndex(String collection, Document index, IndexOptions options) {
        createIndex(getMongoCollection(collection), index, options);
    }

    protected final void createIndex(MongoCollection<Document> collection, Document index) {
        createIndex(collection, index, new IndexOptions().background(true));
    }

    protected final void createIndex(MongoCollection<Document> collection, Document index, IndexOptions options) {
        collection.createIndex(index, options);
    }

    protected final void dropIndex(String collection, Document index) {
        dropIndex(getMongoCollection(collection), index);
    }

    protected final void dropIndex(List<String> collections, Document index) {
        for (String collection : collections) {
            try {
                getMongoCollection(collection).dropIndex(index);
            } catch (Exception e) {
                logger.warn("Could not drop index: {}", e.getMessage());
            }
        }
    }

    protected final void dropIndex(MongoCollection<Document> collection, Document index) {
        try {
            collection.dropIndex(index);
        } catch (Exception e) {
            logger.warn("Could not drop index: {}", e.getMessage());
        }
    }

    protected final void queryMongo(String inputCollectionStr, Bson query, Bson projection, QueryCollectionFunc queryCollectionFunc) {
        MongoCollection<Document> inputCollection = getMongoCollection(inputCollectionStr);

        try (MongoCursor<Document> it = inputCollection
                .find(query)
                .batchSize(batchSize)
                .projection(projection)
                .noCursorTimeout(true)
                .cursor()) {
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

    public MigrationTool setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
}
