package org.opencb.opencga.app.migrations.v2.v2_1_0.catalog.java;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_rc_version_from_migration_runs",
        description = "Remove RC versions from migration runs stored in catalog", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210723,
        deprecatedSince = "v3.0.0")
public class RemoveRCVersionsFromMigrationRuns extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
