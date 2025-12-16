package org.opencb.opencga.app.migrations.v3.v3_2_0.TASK_5964;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_status_name", description = "Remove status name #TASK-5964", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240612,
        deprecatedSince = "5.0.0")
public class RemoveStatusNameMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
