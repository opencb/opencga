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

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id="add_panels_to_interpretations",
        description = "Add panels to Interpretations #1802", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        rank = 16)
public class AddPanelsToInterpretations extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> clinicalCollection = getMongoCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION);

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
