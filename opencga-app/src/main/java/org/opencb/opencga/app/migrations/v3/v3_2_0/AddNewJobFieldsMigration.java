package org.opencb.opencga.app.migrations.v3.v3_2_0;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "add_jobParentId_scheduledStartTime",
        description = "Add 'jobParentId' and 'scheduledStartTime' to existing jobs #TASK-6171 #TASK-6089", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240506,
        deprecatedSince = "5.0.0")
public class AddNewJobFieldsMigration extends MigrationTool {


    @Override
    protected void run() throws Exception {
    }

}
