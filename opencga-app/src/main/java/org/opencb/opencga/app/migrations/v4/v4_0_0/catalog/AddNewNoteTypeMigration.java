package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "addNewNoteType__task_7046",
        description = "Add new Note type #7046", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241030,
        deprecatedSince = "5.0.0")
public class AddNewNoteTypeMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
