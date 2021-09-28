package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id="add_missing_indexes",
        description = "Add missing indexes", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        rank = 21)
public class AddMissingIndexes extends MigrationTool {

    @Override
    protected void run() throws Exception {
        catalogManager.installIndexes(token);

        // Delete old indexes
        MongoCollection<Document> sampleCollection = getMongoCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        sampleCollection.dropIndex("studyUid_1__acl_1__lastOfVersion_1");
    }
}
