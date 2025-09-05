package org.opencb.opencga.app.migrations.v3.v3_2_0.TASK_5964;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_version_to_clinicalAnalysis", description = "Add version to Clinical Analysis #TASK-5964", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240527,
        deprecatedSince = "4.0.0")
public class AddVersionToClinicalAnalysisMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
