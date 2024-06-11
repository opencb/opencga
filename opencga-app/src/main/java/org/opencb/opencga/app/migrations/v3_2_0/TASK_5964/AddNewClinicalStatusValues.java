package org.opencb.opencga.app.migrations.v3_2_0.TASK_5964;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalStatus;
import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;

import java.util.Arrays;
import java.util.List;

@Migration(id = "add_new_clinical_status",
        description = "Add new ClinicalStatus to ClinicalAnalysis and Interpretation, #TASK-5964",
        version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240611)
public class AddNewClinicalStatusValues extends MigrationTool {

    @Override
    protected void run() throws Exception {
        ClinicalAnalysisStudyConfiguration defaultConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();

        Bson query = Filters.exists("status.author", false);
        Bson projection = Projections.include("status", "id");

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION)) {
            migrateCollection(collection, query, projection,
                    (document, bulk) -> {
                        MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                        String clinicalId = document.getString("id");
                        Document status = document.get("status", Document.class);

                        Document newStatus = fillStatusValues("Clinical Analysis", clinicalId, status, defaultConfiguration.getStatus());
                        updateDocument.getSet().put("status", newStatus);

                        Document effectiveUpdate = updateDocument.toFinalUpdateDocument();

                        logger.debug("Updating clinical analysis '{}': {}", document.get("id"), effectiveUpdate.toBsonDocument());
                        bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), effectiveUpdate));
                    });
        }

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION)) {
            migrateCollection(collection, query, projection,
                    (document, bulk) -> {
                        MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                        String interpretationId = document.getString("id");
                        Document status = document.get("status", Document.class);

                        Document newStatus = fillStatusValues("Interpretation", interpretationId, status,
                                defaultConfiguration.getInterpretation().getStatus());
                        updateDocument.getSet().put("status", newStatus);

                        Document effectiveUpdate = updateDocument.toFinalUpdateDocument();

                        logger.debug("Updating interpretation '{}': {}", document.get("id"), effectiveUpdate.toBsonDocument());
                        bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), effectiveUpdate));
                    });
        }
    }

    private Document fillStatusValues(String entity, String id, Document status, List<ClinicalStatusValue> statusValueList) {
        ClinicalStatus clinicalStatus = new ClinicalStatus();
        String clinicalId = status != null ? status.getString("id") : null;
        if (status == null || StringUtils.isEmpty(clinicalId)) {
            logger.warn("Status is empty or does not contain 'id' field. Setting default status value for {} '{}'", entity, id);

        } else {
            for (ClinicalStatusValue clinicalStatusValue : statusValueList) {
                if (clinicalStatusValue.getId().equals(clinicalId)) {
                    clinicalStatus.setId(clinicalStatusValue.getId());
                    clinicalStatus.setDescription(clinicalStatusValue.getDescription());
                    clinicalStatus.setType(clinicalStatusValue.getType());
                    break;
                }
            }
            if (clinicalStatus.getType() == null) {
                logger.warn("Status '{}' not found in the list of available status values. Status type cannot be set for {} '{}'",
                        clinicalId, entity, id);

            }
        }
        clinicalStatus.setDate(TimeUtils.getTime());
        clinicalStatus.setVersion(GitRepositoryState.getInstance().getBuildVersion());
        clinicalStatus.setCommit(GitRepositoryState.getInstance().getCommitId());
        clinicalStatus.setAuthor("opencga");

        Document document = convertToDocument(clinicalStatus);
        return document;
    }

}
