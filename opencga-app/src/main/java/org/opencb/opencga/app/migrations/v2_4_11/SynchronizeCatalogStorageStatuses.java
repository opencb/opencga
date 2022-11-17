package org.opencb.opencga.app.migrations.v2_4_11;

import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;

@Migration(id = "synchronize_catalog_storage_statuses",
        description = "Synchronize catalog internal statuses from storage, #TASK-1304", version = "2.4.11",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20221117)
public class SynchronizeCatalogStorageStatuses extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        VariantStorageManager manager = getVariantStorageManager();
        for (String study : getVariantStorageStudies()) {
            logger.info("Synchronize study '{}'", study);
            manager.synchronizeCatalogStudyFromStorage(study, token);
        }
    }

}
