package org.opencb.opencga.app.migrations.v3_2_0.TASK_5964;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;

import java.util.Arrays;
import java.util.List;

@Migration(id = "update_clinical_study_configuration",
        description = "Setting new default Clinical Study Configuration status values, #TASK-5964",
        version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240610)
public class UpdateClinicalStudyConfiguration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        ClinicalAnalysisStudyConfiguration clinicalAnalysisStudyConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();
        Document clinicalConfigurationDocument = convertToDocument(clinicalAnalysisStudyConfiguration);

        Bson query = new Document();
        Bson projection = Projections.include("configuration", "fqn");

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
            migrateCollection(collection, query, projection,
                    (document, bulk) -> {
                        Document configuration = document.get("configuration", Document.class);
                        MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                        if (configuration == null) {
                            logger.warn("Found empty study configuration in study '{}'. Creating a new one...", document.get("fqn"));
                            updateDocument.getSet().put("configuration", clinicalConfigurationDocument);
                        } else {
                            Object statusObject = configuration.get("status");
                            if (statusObject instanceof List) {
                                // The study seems to be already migrated. Skipping...
                                logger.warn("Study '{}' seems to be already migrated. Skipping...", document.get("fqn"));
                                return;
                            }
                            // Study needs to be migrated
                            logger.info("Migrating study '{}'", document.get("fqn"));
                            updateDocument.getSet().put("configuration.status", clinicalConfigurationDocument.get("status"));
                            updateDocument.getSet().put("configuration.flags", clinicalConfigurationDocument.get("flags"));
                            updateDocument.getSet().put("configuration.interpretation.status", clinicalConfigurationDocument.get("interpretation", Document.class).get("status"));
                        }
                        logger.debug("Updating study '{}': {}", document.get("fqn"), updateDocument.toFinalUpdateDocument());
                        bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                    });
        }
    }

}
