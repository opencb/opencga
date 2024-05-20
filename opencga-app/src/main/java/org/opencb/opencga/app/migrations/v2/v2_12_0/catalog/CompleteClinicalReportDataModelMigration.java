package org.opencb.opencga.app.migrations.v2.v2_12_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "complete_clinical_report_data_model" ,
        description = "Complete Clinical Report data model #TASK-5198",
        version = "2.12.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20231128,
        deprecatedSince = "v3.0.0"
)
public class CompleteClinicalReportDataModelMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
