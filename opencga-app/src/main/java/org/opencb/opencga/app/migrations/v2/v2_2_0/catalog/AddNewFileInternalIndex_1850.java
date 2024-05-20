package org.opencb.opencga.app.migrations.v2.v2_2_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "new_file_internal_index_1850",
        description = "Add new FileInternalVariant and FileInternalAlignment index #1850", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211127, deprecatedSince = "v3.0.0")
public class AddNewFileInternalIndex_1850 extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
