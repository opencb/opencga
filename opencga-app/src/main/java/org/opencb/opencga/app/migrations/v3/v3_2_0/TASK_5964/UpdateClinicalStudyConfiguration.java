package org.opencb.opencga.app.migrations.v3.v3_2_0.TASK_5964;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationRuntimeException;
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
        Bson projection = Projections.include("internal.configuration.clinical", "fqn");

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
            migrateCollection(collection, query, projection,
                    (document, bulk) -> {
                        Document internal = document.get("internal", Document.class);
                        if (internal == null) {
                            throw new MigrationRuntimeException("'internal' field not found in study '" + document.get("fqn") + "'");
                        }
                        Document configuration = internal.get("configuration", Document.class);
                        if (configuration == null) {
                            throw new MigrationRuntimeException("'internal.configuration' field not found in study '" + document.get("fqn") + "'");
                        }
                        Document clinicalDocument = configuration.get("clinical", Document.class);

                        MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                        if (clinicalDocument == null) {
                            logger.warn("Found empty 'internal.configuration.clinical' field in study '{}'. Creating a new one...", document.get("fqn"));
                            updateDocument.getSet().put("internal.configuration.clinical", clinicalConfigurationDocument);
                        } else {
                            Object statusObject = clinicalDocument.get("status");
                            if (statusObject instanceof List) {
                                // The study seems to be already migrated. Skipping...
                                logger.warn("Study '{}' seems to be already migrated. Skipping...", document.get("fqn"));
                                return;
                            }
                            // Study needs to be migrated
                            logger.info("Migrating study '{}'", document.get("fqn"));
                            updateDocument.getSet().put("internal.configuration.clinical.status", clinicalConfigurationDocument.get("status"));
                            updateDocument.getSet().put("internal.configuration.clinical.flags", clinicalConfigurationDocument.get("flags"));
                            updateDocument.getSet().put("internal.configuration.clinical.interpretation.status", clinicalConfigurationDocument.get("interpretation", Document.class).get("status"));
                        }
                        logger.debug("Updating study '{}': {}", document.get("fqn"), updateDocument.toFinalUpdateDocument());
                        bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                    });
        }
    }

}
