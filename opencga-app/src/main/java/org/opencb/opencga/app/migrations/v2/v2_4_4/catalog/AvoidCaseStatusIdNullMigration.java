package org.opencb.opencga.app.migrations.v2.v2_4_4.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "avoidCaseStatusIdNull-1938",
        description = "Avoid nullable status id in Clinical Analysis #TASK-1938", version = "2.4.4",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220914,
        deprecatedSince = "v3.0.0")
public class AvoidCaseStatusIdNullMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
