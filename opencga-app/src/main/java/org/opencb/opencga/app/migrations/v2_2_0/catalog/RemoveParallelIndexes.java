package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_parallel_indexes",
        description = "Remove parallel array indexes #CU-20jc4tx", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220310)
public class RemoveParallelIndexes extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Document newIndex = new Document()
                .append("studyUid", 1)
                .append("_releaseFromVersion", 1)
                .append("_lastOfRelease", 1);
        createIndex(MongoDBAdaptorFactory.SAMPLE_COLLECTION, newIndex);
        createIndex(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, newIndex);
        createIndex(MongoDBAdaptorFactory.FAMILY_COLLECTION, newIndex);
        createIndex(MongoDBAdaptorFactory.PANEL_COLLECTION, newIndex);

        Document oldIndex = new Document()
                .append("studyUid", 1)
                .append("_releaseFromVersion", 1)
                .append("_lastOfRelease", 1)
                .append("_acl", 1);
        dropIndex(MongoDBAdaptorFactory.SAMPLE_COLLECTION, oldIndex);
        dropIndex(MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, oldIndex);
        dropIndex(MongoDBAdaptorFactory.FAMILY_COLLECTION, oldIndex);
        dropIndex(MongoDBAdaptorFactory.PANEL_COLLECTION, oldIndex);
    }
}
