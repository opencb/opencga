package org.opencb.opencga.app.migrations.v2_1_0.storage;


import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
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
        patch = 2,
        rank = 10)
public class DefaultSampleIndexConfiguration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StorageEngineFactory engineFactory = StorageEngineFactory.get(readStorageConfiguration());
        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, engineFactory);

        for (Study study : catalogManager.getStudyManager().search(new Query(), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("fqn", "internal")), token).getResults()) {
            if (study.getInternal().getVariantEngineConfiguration() == null) {
                study.getInternal().setVariantEngineConfiguration(new StudyVariantEngineConfiguration());
            }
            if (study.getInternal().getVariantEngineConfiguration().getSampleIndex() != null) {
                logger.info("Study {} already has a SampleIndex configuration", study.getFqn());
                continue;
            }
            if (variantStorageManager.exists(study.getFqn(), token)) {
                DataStore dataStore = variantStorageManager.getDataStore(study.getFqn(), token);
                VariantStorageEngine engine = engineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
                SampleIndexConfiguration sampleIndexConfiguration = SampleIndexConfiguration.backwardCompatibleConfiguration();
                sampleIndexConfiguration.validate();
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
