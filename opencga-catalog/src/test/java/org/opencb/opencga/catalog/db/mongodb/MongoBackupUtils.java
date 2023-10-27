package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Contains two methods mainly for testing purposes. One to create a dump of the current testing OpenCGA installation and a second one to
 * restore it.
 */
public class MongoBackupUtils {

    private static Logger logger = LoggerFactory.getLogger(MongoBackupUtils.class);

    public static void dump(CatalogManager catalogManager, Path opencgaHome) throws CatalogDBException {
        StopWatch stopWatch = StopWatch.createStarted();
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration())) {
            MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(dbAdaptorFactory.getOrganizationIds().get(0))
                    .getMongoDataStore().getMongoClient();
            MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");
            dumpDatabase.drop();

            Bson emptyBsonQuery = new Document();
            Document databaseSummary = new Document();
            for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
                String databaseName = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId).getMongoDataStore()
                        .getDatabaseName();
                databaseSummary.put(organizationId, databaseName);

                MongoDatabase database = mongoClient.getDatabase(databaseName);
                for (String collection : OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST) {
                    MongoCollection<Document> dbCollection = database.getCollection(collection);
                    MongoCollection<Document> dumpCollection = dumpDatabase.getCollection(organizationId + "__" + collection);

                    try (MongoCursor<Document> iterator = dbCollection.find(emptyBsonQuery).noCursorTimeout(true).iterator()) {
                        List<Document> documentList = new LinkedList<>();
                        while (iterator.hasNext()) {
                            Document document = iterator.next();
                            if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION.equals(collection)) {
                                // Store uri differently
                                String uri = document.getString("uri");
                                String temporalFolder = opencgaHome.getFileName().toString();
                                String replacedUri = uri.replace(temporalFolder, "TEMPORAL_FOLDER_HERE");
                                document.put("uri", replacedUri);
                            }
                            documentList.add(document);
                        }
                        if (!documentList.isEmpty()) {
                            dumpCollection.insertMany(documentList);
                        }
                    }
                }
            }
            dumpDatabase.getCollection("summary").insertOne(databaseSummary);
        }
        logger.info("Database dump created in {} milliseconds.", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public static void restore(CatalogManager catalogManager, Path opencgaHome) throws CatalogDBException, IOException, CatalogIOException {
        StopWatch stopWatch = StopWatch.createStarted();
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration())) {
            MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION)
                    .getMongoDataStore().getMongoClient();
            MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");
            Map<String, String> databaseNames = new HashMap<>();
            try (MongoCursor<Document> mongoIterator = dumpDatabase.getCollection("summary")
                    .find(new Document()).projection(Projections.exclude("_id")).iterator()) {
                while (mongoIterator.hasNext()) {
                    Document document = mongoIterator.next();
                    for (String s : document.keySet()) {
                        databaseNames.put(s, document.getString(s));
                    }
                }
            }

            // First restore the main admin database
            String adminDBName = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION)
                    .getMongoDataStore().getDatabaseName();
            restoreDatabase(catalogManager, opencgaHome, ParamConstants.ADMIN_ORGANIZATION, adminDBName, dbAdaptorFactory);

            for (Map.Entry<String, String> entry : databaseNames.entrySet()) {
                String organizationId = entry.getKey();
                if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                    String databaseName = entry.getValue();
                    restoreDatabase(catalogManager, opencgaHome, organizationId, databaseName, dbAdaptorFactory);
                }
            }
        }
        logger.info("Databases restored in {} milliseconds.", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    private static void restoreDatabase(CatalogManager catalogManager, Path opencgaHome, String organizationId, String databaseName,
                                        MongoDBAdaptorFactory dbAdaptorFactory) throws IOException, CatalogIOException, CatalogDBException {
        MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION).getMongoDataStore().getMongoClient();
        MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");
        Bson emptyBsonQuery = new Document();

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        boolean databaseExists = true;
        if (database.getCollection("organization").countDocuments() == 0) {
            databaseExists = false;
        }

        // Write users folder in disk
        IOManager ioManager = catalogManager.getIoManagerFactory().getDefault();
        Path usersFolder = opencgaHome.resolve("sessions").resolve("orgs").resolve(organizationId).resolve("users");
        ioManager.createDirectory(usersFolder.toUri(), true);

        for (String collection : OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST) {
            MongoCollection<Document> dbCollection = database.getCollection(collection);
            MongoCollection<Document> dumpCollection = dumpDatabase.getCollection(organizationId + "__" + collection);

            dbCollection.deleteMany(emptyBsonQuery);
            try (MongoCursor<Document> iterator = dumpCollection.find(emptyBsonQuery).noCursorTimeout(true).iterator()) {
                List<Document> documentList = new LinkedList<>();
                while (iterator.hasNext()) {
                    Document document = iterator.next();
                    if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)
                            || OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION.equals(collection)
                            || OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)
                            || OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION.equals(collection)) {

                        // Write actual temporal folder in database
                        String uri = document.getString("uri");
                        String temporalFolder = opencgaHome.getFileName().toString();
                        String replacedUri = uri.replace("TEMPORAL_FOLDER_HERE", temporalFolder);
                        document.put("uri", replacedUri);

                        if (OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)) {
                            // Create temporal study folder
                            ioManager.createDirectory(Paths.get(replacedUri).toUri(), true);
                        }
                    }
                    documentList.add(document);
                }
                if (!documentList.isEmpty()) {
                    dbCollection.insertMany(documentList);
                }
            }
        }

        if (!databaseExists) {
            // Database was completely wiped so we need to regenerate non-existing collections and indexes
            logger.info("Database for organization '{}' was wiped. Restoring all indexes again.", organizationId);
            OrganizationMongoDBAdaptorFactory orgFactory = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId);
            orgFactory.createIndexes();
        }
    }

}
