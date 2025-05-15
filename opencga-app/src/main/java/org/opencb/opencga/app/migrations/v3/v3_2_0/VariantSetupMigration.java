package org.opencb.opencga.app.migrations.v3.v3_2_0;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "variant_setup", description = "Add a dummy variant setup for studies with data", version = "3.2.0",
        domain = Migration.MigrationDomain.STORAGE, date = 20240516, deprecatedSince = "4.0.0")
public class VariantSetupMigration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }
}
