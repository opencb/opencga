package org.opencb.opencga.app.migrations.v2.v2_2_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "synchronize_catalog_storage_1850_1851",
        description = "Synchronize catalog storage #1850 #1851", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220118, deprecatedSince = "v3.0.0")
public class SynchronizeCatalogStorage extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
    }

}
