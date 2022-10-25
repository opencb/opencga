package org.opencb.opencga.app.migrations.v2_4_4.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Migration(id = "invalidate_variant_archives_migration-633",
        description = "Invalidate variant archives from variant storage hadoop #TASK-633", version = "2.4.4",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220825)
public class InvalidateVariantArchivesMigration extends StorageMigrationTool {
    @Override
    protected void run() throws Exception {

        for (String project : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(project);
            if (engine.getStorageEngineId().equals("hadoop")) {
                VariantStorageMetadataManager mm = engine.getMetadataManager();
                if (mm.exists()) {
                    for (Integer studyId : mm.getStudyIds()) {
                        for (Integer fileId : mm.getIndexedFiles(studyId)) {
                            mm.updateFileMetadata(studyId, fileId, fileMetadata -> {
                                fileMetadata.getAttributes().put("TASK-633", "AFFECTED");
                            });
                        }
                    }
                }
            }
        }

    }
}
