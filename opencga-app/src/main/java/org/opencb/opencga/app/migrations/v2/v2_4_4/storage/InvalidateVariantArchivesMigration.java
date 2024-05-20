package org.opencb.opencga.app.migrations.v2.v2_4_4.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "invalidate_variant_archives_migration-633",
        description = "Invalidate variant archives from variant storage hadoop #TASK-633", version = "2.4.4",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220825,
        deprecatedSince = "v3.0.0")
public class InvalidateVariantArchivesMigration extends StorageMigrationTool {
    @Override
    protected void run() throws Exception {
    }
}
