package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;

import java.util.*;

@Migration(id = "add_missing_create_interpretation_in_clinical_audit",
        description = "Add missing CREATE_INTERPRETATION audits in ClinicalAnalysis", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211227)
public class AddMissingClinicalAudit extends MigrationTool {

    private final static String SEPARATOR = "____";

    @Override
    protected void run() throws Exception {
        Map<String, List<String>> clinicalInterpretationsMap = new HashMap<>();

        // Extract map of ClinicalAnalysisId - List<InterpretationId>
        queryMongo(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, new Document(MongoDBAdaptor.LAST_OF_VERSION, true),
                Projections.include(Arrays.asList("id", "clinicalAnalysisId", "studyUid")), (doc) -> {
                    String id = doc.getString("id");
                    String clinicalAnalysisId = doc.getString("clinicalAnalysisId");
                    String studyUid = String.valueOf(doc.getLong("studyUid"));
                    String key = clinicalAnalysisId + SEPARATOR + studyUid;
                    if (!clinicalInterpretationsMap.containsKey(key)) {
                        clinicalInterpretationsMap.put(key, new LinkedList<>());
                    }
                    clinicalInterpretationsMap.get(key).add(id);
                });

        // Calculate map of ClinicalAnalysisId - MaximumInterpretationIdCount
        Map<String, Integer> clinicalAuditCount = extractLastInterpretationCounter(clinicalInterpretationsMap);

        String time = TimeUtils.getTime();
        ClinicalAnalysisConverter converter = new ClinicalAnalysisConverter();
        MongoCollection<Document> clinicalCollection = getMongoCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION);
        for (Map.Entry<String, Integer> entry : clinicalAuditCount.entrySet()) {
            logger.debug("Processing entry key '{}'...", entry.getKey());
            String[] split = entry.getKey().split(SEPARATOR);
            String clinicalAnalysisId = split[0];
            long studyUid = Long.parseLong(split[1]);

            Integer createInterpretationCount = entry.getValue();

            queryMongo(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                    new Document("studyUid", studyUid).append("id", clinicalAnalysisId),
                    Projections.include("audit"),
                    (doc) -> {
                        ClinicalAnalysis clinicalAnalysis = converter.convertToDataModelType(doc);

                        int count = 0;
                        for (ClinicalAudit clinicalAudit : clinicalAnalysis.getAudit()) {
                            if (clinicalAudit.getAction().equals(ClinicalAudit.Action.CREATE_INTERPRETATION)) {
                                count++;
                            }
                        }

                        if (createInterpretationCount > count) {
                            List<ClinicalAudit> auditList = new ArrayList<>(clinicalAnalysis.getAudit());

                            while (count < createInterpretationCount) {
                                auditList.add(new ClinicalAudit("opencga", ClinicalAudit.Action.CREATE_INTERPRETATION,
                                        "Migration add_missing_create_interpretation_in_clinical_audit", time));
                                count++;
                            }

                            List<Document> auditDocumentList = convertToDocument(auditList);
                            clinicalCollection.updateOne(new Document("studyUid", studyUid).append("id", clinicalAnalysisId),
                                    Updates.set("audit", auditDocumentList));
                        }
                    });
        }
    }

    private Map<String, Integer> extractLastInterpretationCounter(Map<String, List<String>> clinicalInterpretationsMap) {
        Map<String, Integer> clinicalAuditCount = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : clinicalInterpretationsMap.entrySet()) {
            int size = entry.getValue().size();

            for (String interpretationId : entry.getValue()) {
                String[] split = StringUtils.split(interpretationId, ".");
                String value = split[split.length - 1];
                try {
                    int intValue = Integer.parseInt(value);
                    if (intValue > size) {
                        size = intValue;
                    }
                } catch (NumberFormatException e) {
                    // Ignore error
                    continue;
                }
            }

            clinicalAuditCount.put(entry.getKey(), size);
        }

        return clinicalAuditCount;
    }
}
