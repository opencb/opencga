package org.opencb.opencga.app.migrations.v3.v3_2_0;

import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "internalVariant_3_2_0", description = "Add internal.variant fields to Project and Study #TASK-6219", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240703,
        deprecatedSince = "5.0.0")
public class InternalVariantOperationMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
