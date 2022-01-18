package org.opencb.opencga.app.migrations.v2_2_0.storage;

import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

@Migration(id = "synchronize_catalog_storage_1850_1851",
        description = "Synchronize catalog storage #1850 #1851", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220118)
public class SynchronizeCatalogStorage extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        for (String project : getVariantStorageProjects()) {
            VariantStorageMetadataManager mm = getVariantStorageEngineByProject(project).getMetadataManager();
            for (String study : mm.getStudies().keySet()) {
                logger.info("Synchronize storage-catalog for study {}", study);
                new CatalogStorageMetadataSynchronizer(catalogManager, mm)
                        .synchronizeCatalogStudyFromStorage(study, token);
            }
        }
    }

}
