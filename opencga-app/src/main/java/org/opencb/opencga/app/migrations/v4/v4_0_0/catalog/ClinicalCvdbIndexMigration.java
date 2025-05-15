package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_cvdb_index_to_clinical_analysis",
        description = "Add CVDB index status to Clinical Analysis #TASK-5610", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241118,
        deprecatedSince = "5.0.0")
public class ClinicalCvdbIndexMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
