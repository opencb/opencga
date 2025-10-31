package org.opencb.opencga.app.migrations.v3.v3_1_0;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "hide_secret_key", description = "Hide secret key #TASK-5923", version = "3.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240410,
        deprecatedSince = "4.0.0")
public class AuthOriginSimplificationMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
