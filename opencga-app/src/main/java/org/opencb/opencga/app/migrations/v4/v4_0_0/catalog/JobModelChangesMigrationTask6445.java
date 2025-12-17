package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;


import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "job_model_changes_6445",
        description = "Job data model changes #TASK-6445", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241113,
        deprecatedSince = "5.0.0")
public class JobModelChangesMigrationTask6445 extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
