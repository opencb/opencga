package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_missing_resources", description = "Add missing RESOURCES folder and dependencies #TASK-6442", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241016,
        deprecatedSince = "5.0.0")
public class ResourcesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
