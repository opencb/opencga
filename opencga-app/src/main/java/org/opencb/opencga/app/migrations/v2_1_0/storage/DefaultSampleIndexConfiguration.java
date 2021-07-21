package org.opencb.opencga.app.migrations.v2_1_0.storage;


import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyVariantEngineConfiguration;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

@Migration(id="default_sample_index_configuration", description = "Add a default backward compatible sample index configuration", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 4,
        rank = 16) // Needs to run after StudyClinicalConfigurationRelocation
public class DefaultSampleIndexConfiguration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StorageEngineFactory engineFactory = StorageEngineFactory.get(readStorageConfiguration());
        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, engineFactory);

        for (Study study : catalogManager.getStudyManager().search(new Query(), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("fqn", "internal")), token).getResults()) {
            if (variantStorageManager.exists(study.getFqn(), token)) {
                if (study.getInternal().getConfiguration().getVariantEngine() == null) {
                    study.getInternal().getConfiguration().setVariantEngine(new StudyVariantEngineConfiguration());
                }
                SampleIndexConfiguration sampleIndexConfiguration;
                if (study.getInternal().getConfiguration().getVariantEngine().getSampleIndex() != null) {
                    logger.info("Study {} already has a SampleIndex configuration", study.getFqn());
                    sampleIndexConfiguration = study.getInternal().getConfiguration().getVariantEngine().getSampleIndex();
                    boolean skipUpdateConfiguration = true;
                    if (sampleIndexConfiguration.getAnnotationIndexConfiguration().getTranscriptFlagIndexConfiguration() == null) {
                        logger.info("Missing transcriptFlag");
                        skipUpdateConfiguration = false;
                        sampleIndexConfiguration.getAnnotationIndexConfiguration().setTranscriptFlagIndexConfiguration(
                                new IndexFieldConfiguration(IndexFieldConfiguration.Source.ANNOTATION, "transcriptFlag",
                                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE, "invalid_transcript_flag_index"));
                    }
                    if (skipUpdateConfiguration) {
                        logger.info("Skip study");
                        continue;
                    }
                } else {
                    sampleIndexConfiguration = SampleIndexConfiguration.backwardCompatibleConfiguration();
                    sampleIndexConfiguration.validate();
                }

                DataStore dataStore = variantStorageManager.getDataStore(study.getFqn(), token);
                VariantStorageEngine engine = engineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
                engine.getMetadataManager().updateStudyMetadata(study.getFqn(), studyMetadata -> {
                    if (CollectionUtils.isEmpty(studyMetadata.getSampleIndexConfigurations())) {
                        studyMetadata.setSampleIndexConfigurations(new ArrayList<>());
                        studyMetadata.getSampleIndexConfigurations().add(
                                new StudyMetadata.SampleIndexConfigurationVersioned(sampleIndexConfiguration, 1, Date.from(Instant.now())));
                    }
                    return studyMetadata;
                });
                catalogManager.getStudyManager()
                        .setVariantEngineConfigurationSampleIndex(study.getFqn(), sampleIndexConfiguration, token);
            }
        }
    }
}
