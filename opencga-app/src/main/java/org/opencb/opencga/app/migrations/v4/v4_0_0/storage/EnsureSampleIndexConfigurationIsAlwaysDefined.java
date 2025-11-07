package org.opencb.opencga.app.migrations.v4.v4_0_0.storage;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

@Migration(id = "ensure_sample_index_configuration_is_defined",
        description = "Ensure that the SampleIndexConfiguration object is correctly defined. #TASK-6765", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 2,
        date = 20240910)
public class EnsureSampleIndexConfigurationIsAlwaysDefined extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {

        for (String variantStorageProject : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(variantStorageProject);
            logger.info("Checking project '" + variantStorageProject + "'");
            if (engine.getMetadataManager().exists()) {
                logger.info("Project '" + variantStorageProject + "' exists in variant storage. Checking studies.");
                for (Integer studyId : engine.getMetadataManager().getStudyIds()) {
                    StudyMetadata studyMetadata = engine.getMetadataManager().getStudyMetadata(studyId);
                    if (CollectionUtils.isEmpty(studyMetadata.getSampleIndexConfigurations())) {
                        studyMetadata = engine.getMetadataManager().updateStudyMetadata(studyId, sm -> {
                            if (CollectionUtils.isEmpty(sm.getSampleIndexConfigurations())) {
                                List<StudyMetadata.SampleIndexConfigurationVersioned> configurations = new ArrayList<>(1);
                                logger.info("Creating default SampleIndexConfiguration for study '" + sm.getName() + "'"
                                        + " (" + studyId + ")");
                                configurations.add(new StudyMetadata.SampleIndexConfigurationVersioned(
                                        preFileDataConfiguration(),
                                        StudyMetadata.DEFAULT_SAMPLE_INDEX_VERSION,
                                        Date.from(Instant.now()), StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE));
                                sm.setSampleIndexConfigurations(configurations);
                            }
                        });
                    }
                    Study study = catalogManager.getStudyManager().get(studyMetadata.getName(), null, token).first();
                    SampleIndexConfiguration sampleIndexConfFromStorage = studyMetadata.getSampleIndexConfigurationLatest().getConfiguration();
                    SampleIndexConfiguration sampleIndexConfFromCatalog = orDefault(() -> study.getInternal()
                            .getConfiguration().getVariantEngine().getSampleIndex(), null);

                    if (sampleIndexConfFromStorage.equals(sampleIndexConfFromCatalog)) {
                        logger.info("SampleIndexConfiguration for study '" + studyMetadata.getName() + "' (" + studyId + ") is up to date.");
                    } else {
                        logger.info("Updating SampleIndexConfiguration for study '" + studyMetadata.getName() + "' (" + studyId + ")");
                        catalogManager.getStudyManager().setVariantEngineConfigurationSampleIndex(studyMetadata.getName(),
                                sampleIndexConfFromStorage, token);
                    }
                }
            } else {
                logger.info("Project '" + variantStorageProject + "' does not exist in variant storage. Skipping.");
            }
        }
    }

    public static <T> T orDefault(Supplier<T> supplier, T defaultValue) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static SampleIndexConfiguration preFileDataConfiguration() {
        // If missing, it was assuming cellbase v5
        SampleIndexConfiguration sampleIndexConfiguration = SampleIndexConfiguration.defaultConfiguration(false);
        sampleIndexConfiguration.getFileDataConfiguration().setIncludeOriginalCall(false);
        sampleIndexConfiguration.getFileDataConfiguration().setIncludeSecondaryAlternates(false);
        return sampleIndexConfiguration;
    }
}
