package org.opencb.opencga.app.migrations.v2.v2_3_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id="add_missing_columns_to_phoenix_TASK-789", description = "Add missing 1000G columns to phoenix #TASK-789",
        version = "2.3.0", domain = Migration.MigrationDomain.STORAGE, date = 20220614, deprecatedSince = "3.0.0"
)
public class AddMissingColumnsToPhoenix extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
