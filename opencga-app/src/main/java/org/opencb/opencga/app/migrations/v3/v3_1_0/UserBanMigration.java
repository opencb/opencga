package org.opencb.opencga.app.migrations.v3.v3_1_0;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "addFailedLoginAttemptsMigration", description = "Add failedAttempts to User #TASK-6013", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240419, patch = 2,
        deprecatedSince = "4.0.0")
public class UserBanMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {

    }

}
