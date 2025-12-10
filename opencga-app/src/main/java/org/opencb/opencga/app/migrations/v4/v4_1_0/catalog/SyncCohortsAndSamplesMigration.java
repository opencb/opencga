package org.opencb.opencga.app.migrations.v4.v4_1_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "syncCohortsAndSamplesMigration__task_7671" ,
        description = "Remove references in Sample from non-existing cohorts.",
        version = "4.1.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20250605,
        deprecatedSince = "5.0.0"
)
public class SyncCohortsAndSamplesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
