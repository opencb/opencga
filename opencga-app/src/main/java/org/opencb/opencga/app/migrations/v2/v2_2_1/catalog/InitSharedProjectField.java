package org.opencb.opencga.app.migrations.v2.v2_2_1.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "init_shared_project",
        description = "Initialise sharedProjects #TASK-702", version = "2.2.1",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220502, deprecatedSince = "3.0.0")
public class InitSharedProjectField extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
