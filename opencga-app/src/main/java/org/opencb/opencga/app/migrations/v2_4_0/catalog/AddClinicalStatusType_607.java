package org.opencb.opencga.app.migrations.v2_4_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalStatusValue;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_clinical_status_type_TASK-607",
        description = "Automatically close cases depending on the new clinical status type #TASK-607", version = "2.4.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220610)
public class AddClinicalStatusType_607 extends MigrationTool {

    private void addStatusType(Document status, Map<String, String> defaultStatusTypeMap) {
        for (Map.Entry<String, Object> entry : status.entrySet()) {
            List<Document> statusValueList = (List<Document>) entry.getValue();
            for (Document statusValue : statusValueList) {
                String id = statusValue.getString("id");
                statusValue.put("type", defaultStatusTypeMap.getOrDefault(id, "UNKNOWN"));
            }
        }
    }

    @Override
    protected void run() throws Exception {
        ClinicalAnalysisStudyConfiguration clinicalConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();
        Map<String, String> caseStatusTypeMap = new HashMap<>();
        Map<String, String> interpretationStatusTypeMap = new HashMap<>();

        // Default statuses and types are the same for every ClinicalAnalysis.Type, so we can take this shortcut
        List<ClinicalStatusValue> statuses = clinicalConfiguration.getStatus().get(ClinicalAnalysis.Type.FAMILY);
        for (ClinicalStatusValue clinicalStatusValue : statuses) {
            caseStatusTypeMap.put(clinicalStatusValue.getId(), clinicalStatusValue.getType().name());
        }
        statuses = clinicalConfiguration.getInterpretation().getStatus().get(ClinicalAnalysis.Type.FAMILY);
        for (ClinicalStatusValue status : statuses) {
            interpretationStatusTypeMap.put(status.getId(), status.getType().name());
        }

        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document("internal.configuration.clinical.status.FAMILY.type", new Document("$exists", false)),
                Projections.include("internal.configuration.clinical"),
                (doc, bulk) -> {
                    Document internal = doc.get("internal", Document.class);
                    Document configuration = internal.get("configuration", Document.class);
                    Document clinicalConfig = configuration.get("clinical", Document.class);

                    // ClinicalAnalysis status type
                    Document status = clinicalConfig.get("status", Document.class);
                    addStatusType(status, caseStatusTypeMap);

                    Document interpretation = clinicalConfig.get("interpretation", Document.class);
                    status = interpretation.get("status", Document.class);
                    addStatusType(status, interpretationStatusTypeMap);

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", doc.get("_id")),
                                    new Document("$set", new Document("internal.configuration.clinical", clinicalConfig))
                            )
                    );
                }
        );
    }

}
