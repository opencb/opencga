package org.opencb.opencga.app.migrations.v3.v3_2_0;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.study.VariantSetupResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.util.LinkedHashSet;
import java.util.List;

@Migration(id = "variant_setup", description = "Add a dummy variant setup for studies with data", version = "3.2.0",
        domain = Migration.MigrationDomain.STORAGE, date = 20240516)
public class VariantSetupMigration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        VariantStorageManager variantStorageManager = getVariantStorageManager();
        List<String> storageStudies = getVariantStorageStudies();
        if (storageStudies.isEmpty()) {
            logger.info("No studies with variant storage found on organization '{}'", organizationId);
            return;
        }
        for (String study : storageStudies) {
            logger.info("--- Checking study '{}'", study);
            if (variantStorageManager.hasVariantSetup(study, token)) {
                logger.info("Study '{}' already has a variant setup", study);
                continue;
            }

            String projectFqn = catalogManager.getStudyManager().getProjectFqn(study);
            VariantStorageMetadataManager metadataManager = getVariantStorageEngineByProject(projectFqn).getMetadataManager();
            int studyId = metadataManager.getStudyId(study);
            LinkedHashSet<Integer> indexedFiles = metadataManager.getIndexedFiles(studyId);
            if (indexedFiles.isEmpty()) {
                logger.info("Study '{}' does not have any indexed files. Skipping variant setup", study);
                continue;
            }
            logger.info("Study '{}' doesn't have a variant setup, but it has {} indexed files. Creating a dummy variant setup",
                    study, indexedFiles.size());
            logger.info("Creating a dummy variant setup for study '{}'", study);
            VariantSetupResult dummy = new VariantSetupResult();
            dummy.setDate(TimeUtils.getTime());
            dummy.setUserId(catalogManager.getUserManager().getUserId(token));
            dummy.setParams(new ObjectMap("executed_from_migration", getId()));
            dummy.setStatus(VariantSetupResult.Status.READY);
            dummy.setOptions(new ObjectMap());
            catalogManager.getStudyManager().setVariantEngineSetupOptions(study, dummy, token);
        }
    }
}
