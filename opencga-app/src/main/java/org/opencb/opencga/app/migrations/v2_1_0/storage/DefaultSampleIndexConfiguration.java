package org.opencb.opencga.app.migrations.v2_1_0.storage;


import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyVariantEngineConfiguration;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

@Migration(id="default_sample_index_configuration", description = "Add a default backward compatible sample index configuration", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 6,
        rank = 16) // Needs to run after StudyClinicalConfigurationRelocation
public class DefaultSampleIndexConfiguration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        VariantStorageManager variantStorageManager = getVariantStorageManager();

        for (Study study : catalogManager.getStudyManager().search(new Query(), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("fqn", "internal")), token).getResults()) {
            if (variantStorageManager.exists(study.getFqn(), token)) {
                DataStore dataStore = variantStorageManager.getDataStore(study.getFqn(), token);
                VariantStorageEngine engine = getVariantStorageEngineFactory()
                        .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
                StudyMetadata metadata = engine.getMetadataManager().getStudyMetadata(study.getFqn());
                if (study.getInternal().getConfiguration().getVariantEngine() == null) {
                    study.getInternal().getConfiguration().setVariantEngine(new StudyVariantEngineConfiguration());
                }

                boolean updateConfiguration = false;

                SampleIndexConfiguration sampleIndexConfiguration;
                if (CollectionUtils.isEmpty(metadata.getSampleIndexConfigurations())) {
                    updateConfiguration = true;
                    sampleIndexConfiguration = SampleIndexConfiguration.backwardCompatibleConfiguration();
                    sampleIndexConfiguration.validate();
                } else {
                    logger.info("Study {} already has a SampleIndex configuration", study.getFqn());
                    sampleIndexConfiguration = metadata.getSampleIndexConfigurationLatest().getConfiguration();

                    if (study.getInternal().getConfiguration().getVariantEngine().getSampleIndex() == null) {
                        // Missing sample index configuration in catalog
                        updateConfiguration = true;
                    } else {
                        SampleIndexConfiguration catalogSampleIndexConfiguration
                                = study.getInternal().getConfiguration().getVariantEngine().getSampleIndex();
                        // Different sample index configuration
                        if (!catalogSampleIndexConfiguration.equals(sampleIndexConfiguration)) {
                            updateConfiguration = true;
                        }
                    }
                    if (sampleIndexConfiguration.getAnnotationIndexConfiguration().getTranscriptFlagIndexConfiguration() == null) {
                        logger.info("Missing transcriptFlag");
                        updateConfiguration = true;
                        sampleIndexConfiguration.getAnnotationIndexConfiguration().setTranscriptFlagIndexConfiguration(
                                new IndexFieldConfiguration(IndexFieldConfiguration.Source.ANNOTATION, "transcriptFlag",
                                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE, "invalid_transcript_flag_index"));
                    }
                    if (sampleIndexConfiguration.getAnnotationIndexConfiguration().getTranscriptCombination() == null) {
                        // If null, this combination was not computed.
                        sampleIndexConfiguration.getAnnotationIndexConfiguration().setTranscriptCombination(false);
                        updateConfiguration = true;
                    }
                }

                if (!updateConfiguration) {
                    logger.info("Skip study");
                    continue;
                }

                engine.getMetadataManager().updateStudyMetadata(study.getFqn(), studyMetadata -> {
                    if (CollectionUtils.isEmpty(studyMetadata.getSampleIndexConfigurations())) {
                        studyMetadata.setSampleIndexConfigurations(new ArrayList<>());
                        studyMetadata.getSampleIndexConfigurations().add(
                                new StudyMetadata.SampleIndexConfigurationVersioned(sampleIndexConfiguration, 1, Date.from(Instant.now())));
                    } else {
                        int size = studyMetadata.getSampleIndexConfigurations().size();
                        studyMetadata.getSampleIndexConfigurations().get(size - 1).setConfiguration(sampleIndexConfiguration);
                    }
                    return studyMetadata;
                });
                catalogManager.getStudyManager()
                        .setVariantEngineConfigurationSampleIndex(study.getFqn(), sampleIndexConfiguration, token);
            }
        }
    }
}
