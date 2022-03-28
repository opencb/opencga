package org.opencb.opencga.app.migrations.v2_2_1.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Migration(id = "sample_index_configuration_add_missing_status",
        description = "Sample index configuration add missing status #TASK-512", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        date = 20220328)
public class SampleIndexConfigurationAddMissingStatus extends StorageMigrationTool {
    @Override
    protected void run() throws Exception {
        for (String project : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(project);
            VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
            for (Integer studyId : metadataManager.getStudyIds()) {
                StudyMetadata study = metadataManager.getStudyMetadata(studyId);
                if (study.getSampleIndexConfigurations() != null &&
                        study.getSampleIndexConfigurations().stream().anyMatch(sc -> sc.getStatus() == null)) {
                    metadataManager.updateStudyMetadata(studyId, sm -> {
                        for (StudyMetadata.SampleIndexConfigurationVersioned sc : sm.getSampleIndexConfigurations()) {
                            if (sc.getStatus() == null) {
                                sc.setStatus(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE);
                            }
                        }
                    });
                }
            }
        }
    }
}
