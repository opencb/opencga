package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.conversions.Bson;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;

import java.util.LinkedList;
import java.util.List;

@Migration(id = "init_sampleIndexConfiguration-TASK550",
        description = "Initialise SampleIndexConfiguration in Study internal #TASK-550", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220502)
public class InitSampleIndexConfiguration extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {

        Bson query = Filters.or(
                Filters.exists("internal.configuration.variantEngine.sampleIndex", false),
                Filters.eq("internal.configuration.variantEngine.sampleIndex", null)
        );
        Bson projection = Projections.include("fqn");
        List<String> fqnList = new LinkedList<>();
        queryMongo(MongoDBAdaptorFactory.STUDY_COLLECTION, query, projection, (d) -> fqnList.add(d.getString("fqn")));

        for (String study : fqnList) {
            VariantStorageManager manager = getVariantStorageManager();
            SampleIndexConfiguration configuration;
            if (manager.exists(study, token)) {
                configuration = manager.getStudyMetadata(study, token).getSampleIndexConfigurationLatest(true).getConfiguration();
            } else {
                configuration = SampleIndexConfiguration.defaultConfiguration();
            }
            // add
            catalogManager.getStudyManager()
                    .setVariantEngineConfigurationSampleIndex(study, configuration, token);
        }
    }

}
