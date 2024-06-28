package org.opencb.opencga.app.migrations.v2.v2_3_0.catalog;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_variant_storage_tool_ids_TASK-705",
        description = "Rename variant storage tool ids #TASK-705", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220506,
        deprecatedSince = "3.0.0")
public class RenameDeprecatedVariantStorageToolId extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
