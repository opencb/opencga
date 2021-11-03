package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;


import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "move_study_clinical_config_to_internal",
        description = "Move Study ClinicalConfiguration to internal.configuration", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210708)
public class StudyClinicalConfigurationRelocation extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document("configuration", new Document("$exists", true)),
                Projections.include("_id", "configuration", "internal"),
                (study, bulk) -> {
                    Document configuration = study.get("configuration", Document.class);
                    Document internal = study.get("internal", Document.class);

                    Document variantEngineConfiguration = internal.get("variantEngineConfiguration", Document.class);

                    // Remove variantEngineConfiguration from internal
                    internal.remove("variantEngineConfiguration");

                    // Add variantEngineConfiguration to configuration as variantEngine
                    configuration.put("variantEngine", variantEngineConfiguration);

                    // Add configuration to internal
                    internal.put("configuration", configuration);

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", study.get("_id")),
                                    new Document()
                                            .append("$set", new Document("internal", internal))
                                            .append("$unset", new Document("configuration", ""))
                            )
                    );
                }
        );
    }
}
