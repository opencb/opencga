package org.opencb.opencga.catalog.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.migration.MigrationRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MigrationTool {

    protected Configuration configuration;
    protected CatalogManager catalogManager;
    protected MongoDBAdaptorFactory dbAdaptorFactory;
    protected MigrationRun migrationRun;
    protected String organizationId;
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
                            MigrationRun migrationRun, String organizationId, Path appHome, ObjectMap params, String token) {
        this.configuration = configuration;
        this.catalogManager = catalogManager;
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.migrationRun = migrationRun;
        this.organizationId = organizationId;
        this.appHome = appHome;
        this.params = params;
        this.token = token;
    }

    public final void execute() throws MigrationException {
        try {
            Migration annotation = getAnnotation();
            if (StringUtils.isNotEmpty(annotation.deprecatedSince())) {
                throw MigrationException.deprecatedMigration(annotation);
            }
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

    @FunctionalInterface
    protected interface MigrateCollectionFunc {
        void accept(Document document, List<WriteModel<Document>> bulk);
    }

    @FunctionalInterface
    protected interface QueryCollectionFunc {
        void accept(Document document);
    }

    protected final void migrateCollection(String collection, Bson query, Bson projection, MigrateCollectionFunc migrateFunc)
            throws CatalogDBException {
        migrateCollection(collection, collection, query, projection, migrateFunc);
    }

    protected final void migrateCollection(List<String> collections, Bson query, Bson projection, MigrateCollectionFunc migrateFunc)
            throws CatalogDBException {
        for (String collection : collections) {
            privateLogger.info("Starting migration in {}", collection);
            migrateCollection(collection, collection, query, projection, migrateFunc);
        }
    }

    protected final void migrateCollection(String inputCollection, String outputCollection, Bson query, Bson projection,
                                           MigrateCollectionFunc migrateFunc) throws CatalogDBException {
        migrateCollection(getMongoCollection(inputCollection),
                getMongoCollection(outputCollection), query, projection, migrateFunc);
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

    protected void copyData(Bson query, String sourceCol, String targetCol) throws CatalogDBException {
        MongoCollection<Document> sourceMongoCollection = getMongoCollection(sourceCol);
        MongoCollection<Document> targetMongoCollection = getMongoCollection(targetCol);
        copyData(query, sourceMongoCollection, targetMongoCollection);
    }

    protected void copyData(Bson query, MongoCollection<Document> sourceCol, MongoCollection<Document> targetCol) {
        // Move data to the new collection
        logger.info("Copying data from {} to {}", sourceCol.getNamespace(), targetCol.getNamespace());
        migrateCollection(sourceCol, targetCol, query, Projections.exclude("_id"),
                (document, bulk) -> bulk.add(new InsertOneModel<>(document)));
    }

    protected void moveData(Bson query, MongoCollection<Document> sourceCol, MongoCollection<Document> targetCol) {
        copyData(query, sourceCol, targetCol);
        // Remove data from the source collection
        sourceCol.deleteMany(query);
    }

    protected final void createIndex(String collection, Document index) throws CatalogDBException {
        createIndex(getMongoCollection(collection), index, new IndexOptions().background(true));
    }

    protected final void createIndex(List<String> collections, Document index) throws CatalogDBException {
        createIndexes(collections, Collections.singletonList(index));
    }

    protected final void createIndexes(List<String> collections, List<Document> indexes) throws CatalogDBException {
        for (String collection : collections) {
            for (Document index : indexes) {
                createIndex(getMongoCollection(collection), index, new IndexOptions().background(true));
            }
        }
    }

    protected final void createIndex(String collection, Document index, IndexOptions options)
            throws CatalogDBException {
        createIndex(getMongoCollection(collection), index, options);
    }

    protected final void createIndex(MongoCollection<Document> collection, Document index) {
        createIndex(collection, index, new IndexOptions().background(true));
    }

    protected final void createIndex(MongoCollection<Document> collection, Document index, IndexOptions options) {
        collection.createIndex(index, options);
    }

    protected final void dropIndex(String collection, Document index) throws CatalogDBException {
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

    protected final void queryMongo(String inputCollectionStr, Bson query, Bson projection, QueryCollectionFunc queryCollectionFunc)
            throws CatalogDBException {
        MongoCollection<Document> inputCollection = getMongoCollection(inputCollectionStr);
        queryMongo(inputCollection, query, projection, queryCollectionFunc);
    }

    protected final void queryMongo(MongoCollection<Document> inputCollection, Bson query, Bson projection,
                                    QueryCollectionFunc queryCollectionFunc) throws CatalogDBException {
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

    protected final MongoCollection<Document> getMongoCollection(String collectionName) throws CatalogDBException {
        return dbAdaptorFactory.getMongoDataStore(organizationId).getDb().getCollection(collectionName);
    }

    protected final MongoCollection<Document> getMongoCollection(String organization, String collectionName) throws CatalogDBException {
        return dbAdaptorFactory.getMongoDataStore(organization).getDb().getCollection(collectionName);
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
