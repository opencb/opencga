package org.opencb.opencga.app.migrations.v2.v2_12_5.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id="add_missing_column_to_phoenix_TASK-6005", description = "Add missing ALLELES column to phoenix #TASK-6005",
        version = "2.12.5", domain = Migration.MigrationDomain.STORAGE, date = 20240510, deprecatedSince = "4.0.0"
)
public class AddAllelesColumnToPhoenix extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}