package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "autoincrement_version_TASK-323",
        description = "Reorganise data in new collections (autoincrement version) #TASK-323", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        offline = true,
        date = 20220512)
public class AutoIncrementVersion extends MigrationTool {

    private void reorganiseData(String collection, String archiveCollection) {
        logger.info("Copying data from '{}' to '{}' collection", collection, archiveCollection);
        // Replicate all the data in the archive collection
        migrateCollection(collection, archiveCollection, new Document(), new Document(),
                ((document, bulk) -> bulk.add(new InsertOneModel<>(GenericDocumentComplexConverter.replaceDots(document)))));

        // Delete all documents that are not lastOfVersion from main collection
        logger.info("Removing outdated data (_lastOfVersion = false) from '{}' collection", collection);
        MongoCollection<Document> mongoCollection = getMongoCollection(collection);
        DeleteResult deleteResult = mongoCollection.deleteMany(Filters.eq(MongoDBAdaptor.LAST_OF_VERSION, false));
        logger.info("{} documents removed from '{}' collection", deleteResult.getDeletedCount(), collection);
    }

    @Override
    protected void run() throws Exception {
        // Step 1: Create all indexes
        logger.info("Creating indexes in new archive collections...");
        catalogManager.installIndexes(token);

        // Step 2: Replicate data to archive collections and delete documents that are not the last of version from main collection
        reorganiseData(MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.FAMILY_COLLECTION, MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.PANEL_COLLECTION, MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION);

        // Reduce batch size for interpretations
        setBatchSize(100);
        reorganiseData(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, MongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION);
    }

}
