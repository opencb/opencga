package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_deleted_collections_TASK-359",
        description = "Rename deleted collections #TASK-359", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220502,
        deprecatedSince = "3.0.0")
public class RenameDeletedCollections extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
