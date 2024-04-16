package org.opencb.opencga.app.migrations.v2_12_4.catalog;

import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "fix_status_indexes" ,
        description = "Replace 'status.name' indexes for 'status.id'",
        version = "2.12.4",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20240328
)
public class FixStatusIndexesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Document statusIndex = new Document()
                .append("status.name", 1)
                .append("studyUid", 1);
        dropIndex(Arrays.asList(MongoDBAdaptorFactory.JOB_COLLECTION, MongoDBAdaptorFactory.FILE_COLLECTION,
                MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                MongoDBAdaptorFactory.COHORT_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, MongoDBAdaptorFactory.FAMILY_COLLECTION,
                MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, MongoDBAdaptorFactory.PANEL_COLLECTION,
                MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION), statusIndex);

        Document internalStatusIndex = new Document()
                .append("internal.status.name", 1)
                .append("studyUid", 1);
        dropIndex(Arrays.asList(MongoDBAdaptorFactory.JOB_COLLECTION, MongoDBAdaptorFactory.FILE_COLLECTION,
                MongoDBAdaptorFactory.SAMPLE_COLLECTION, MongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                MongoDBAdaptorFactory.COHORT_COLLECTION, MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                MongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, MongoDBAdaptorFactory.FAMILY_COLLECTION,
                MongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, MongoDBAdaptorFactory.PANEL_COLLECTION,
                MongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION, MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, MongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION),
                internalStatusIndex);

        catalogManager.installIndexes(token);
    }

}
