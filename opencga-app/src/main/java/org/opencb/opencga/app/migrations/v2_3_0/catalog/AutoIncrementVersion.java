package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "autoincrement_version_TASK-323",
        description = "Reorganise data in new collections (autoincrement version) #TASK-323", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220512)
public class AutoIncrementVersion extends MigrationTool {

    private void reorganiseData(String collection, String archiveCollection) {
        // Replicate all the data in the archive collection
        migrateCollection(collection, archiveCollection, new Document(), new Document(),
                ((document, bulk) -> bulk.add(new InsertOneModel<>(document))));

        // Delete all documents that are not lastOfVersion from main collection
        MongoCollection<Document> mongoCollection = getMongoCollection(collection);
        mongoCollection.deleteMany(Filters.eq(MongoDBAdaptor.LAST_OF_VERSION, false));
    }

    @Override
    protected void run() throws Exception {
        // Step 1: Create all indexes
        catalogManager.installIndexes(token);

        // Step 2: Replicate data to archive collections and delete documents that are not the last of version from main collection
        reorganiseData(MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.FAMILY_COLLECTION, MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.PANEL_COLLECTION, MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION);
        reorganiseData(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, MongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION);
    }

}
