package org.opencb.opencga.app.migrations.v2.v2_12_6;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "syncCohortsAndSamplesMigration" ,
        description = "Sync array of samples from cohort with array of cohortIds from Sample.",
        version = "2.12.6",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20240621,
        patch = 2, // TASK-6998
        deprecatedSince = "4.0.0"
)
public class SyncCohortsAndSamplesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
