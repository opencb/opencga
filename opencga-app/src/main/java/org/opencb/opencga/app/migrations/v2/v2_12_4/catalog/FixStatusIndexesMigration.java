package org.opencb.opencga.app.migrations.v2.v2_12_4.catalog;

import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
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
        dropIndex(Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION, OrganizationMongoDBAdaptorFactory.FILE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION, OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION, OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION,
                OrganizationMongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION), statusIndex);

        Document internalStatusIndex = new Document()
                .append("internal.status.name", 1)
                .append("studyUid", 1);
        dropIndex(Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION, OrganizationMongoDBAdaptorFactory.FILE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION, OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION, OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION,
                OrganizationMongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION, OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION, OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION),
                internalStatusIndex);

        catalogManager.installIndexes(token);
    }

}
