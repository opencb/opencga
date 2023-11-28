package org.opencb.opencga.app.migrations.v2_12_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

@Migration(id = "complete_clinical_report_data_model" ,
        description = "Complete Clinical Report data model #TASK-5198",
        version = "2.12.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20231128
)
public class CompleteClinicalReportDataModelMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(
                Arrays.asList(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                Filters.exists("analyst"),
                Projections.include(Collections.singletonList("analyst")),
                (document, bulk) -> {

                });
    }

}
