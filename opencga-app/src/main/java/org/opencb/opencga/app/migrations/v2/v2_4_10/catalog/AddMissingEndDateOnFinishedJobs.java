package org.opencb.opencga.app.migrations.v2.v2_4_10.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_missing_endDate_on_finished_jobs" ,
        description = "Add missing end date on finished jobs",
        version = "2.4.10",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20221026,
        deprecatedSince = "v3.0.0"
)
public class AddMissingEndDateOnFinishedJobs extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
