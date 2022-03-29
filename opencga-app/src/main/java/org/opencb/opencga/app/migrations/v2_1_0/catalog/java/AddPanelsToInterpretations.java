package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_panels_to_interpretations",
        description = "Add panels to Interpretations #1802", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20210713)
public class AddPanelsToInterpretations extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> clinicalCollection = getMongoCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION);

        /*
            1. Check if there is any orphan interpretation (attached to an unknown clinical analysis)
        */
        // Obtain all clinicalAnalysisIds (study - [clinicalIds])
        Map<String, Set<String>> clinicalMap = new HashMap<>();
        queryMongo(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, new Document(),
                Projections.include(Arrays.asList("id", "studyUid")), (doc) -> {
                    String id = doc.getString("id");
                    String studyUid = String.valueOf(doc.getLong("studyUid"));

                    if (!clinicalMap.containsKey(studyUid)) {
                        clinicalMap.put(studyUid, new HashSet<>());
                    }
                    clinicalMap.get(studyUid).add(id);
                });

        List<Object> interpretationsToRemove = new LinkedList<>();
        // Iterate over all interpretations and remove orphan ones
        queryMongo(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION, new Document(),
                Projections.include(Arrays.asList("_id", "clinicalAnalysisId", "studyUid")), (doc) -> {
                    Object interpretationId = doc.get("_id");
                    String clinicalId = doc.getString("clinicalAnalysisId");
                    String studyUid = String.valueOf(doc.getLong("studyUid"));

                    if (!clinicalMap.containsKey(studyUid) || !clinicalMap.get(studyUid).contains(clinicalId)) {
                        interpretationsToRemove.add(interpretationId);
                    }
                });

        if (!interpretationsToRemove.isEmpty()) {
            MongoCollection<Document> interpretationCollection = getMongoCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION);
            List<List<Object>> bulkList = generateBulks(interpretationsToRemove);
            logger.info("Removing " + interpretationsToRemove.size() + " orphan interpretations...");
            for (List<Object> list : bulkList) {
                interpretationCollection.deleteMany(Filters.in("_id", list));
            }
        }

        /*
            2. Add panels to Interpretations
        */
        migrateCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                new Document(InterpretationDBAdaptor.QueryParams.PANELS.key(), new Document("$exists", false)),
                Projections.include("_id", InterpretationDBAdaptor.QueryParams.ID.key(),
                        InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key(),
                        InterpretationDBAdaptor.QueryParams.STUDY_UID.key()),
                (interpretation, bulk) -> {

                    // Get panels from related Clinical Analysis
                    String interpretationId = interpretation.getString(InterpretationDBAdaptor.QueryParams.ID.key());
                    String clinicalId = interpretation.getString(InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key());
                    long studyUid = interpretation.getLong(InterpretationDBAdaptor.QueryParams.STUDY_UID.key());

                    Bson query = Filters.and(
                            Filters.eq(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid),
                            Filters.eq(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), clinicalId)
                    );
                    Bson projection = Projections.include("_id", ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key());

                    try (MongoCursor<Document> cursor = clinicalCollection.find(query).projection(projection).cursor()) {
                        if (cursor.hasNext()) {
                            Document clinicalDocument = cursor.next();
                            if (cursor.hasNext()) {

                                throw new RuntimeException("Found more than one Clinical Analysis '" + clinicalId + "' for Interpretation '"
                                        + interpretationId + "' from " + "studyUid " + studyUid);
                            }

                            List<Document> panels = clinicalDocument.getList(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), Document.class);
                            // Set same panels from ClinicalAnalysis to the Interpretation
                            bulk.add(new UpdateOneModel<>(
                                            eq("_id", interpretation.get("_id")),
                                            new Document("$set", new Document(InterpretationDBAdaptor.QueryParams.PANELS.key(), panels))
                                    )
                            );
                        } else {
                            throw new RuntimeException("Could not find Clinical Analysis '" + clinicalId + "' for Interpretation '"
                                    + interpretationId + "' from " + "studyUid " + studyUid);
                        }
                    }
                }
        );
    }
}
