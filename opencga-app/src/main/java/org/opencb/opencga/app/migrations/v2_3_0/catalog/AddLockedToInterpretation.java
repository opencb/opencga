package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "add_locked_to_interpretation_TASK-552",
        description = "Add new 'locked' field to Interpretation #TASK-552", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220401)
public class AddLockedToInterpretation extends MigrationTool {

    @Override
    protected void run() throws Exception {
        logger.info("Creating new index in Interpretation collection");
        MongoCollection<Document> interpretationCollection = getMongoCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION);
        Document lockedIndex = new Document()
                .append("locked", 1)
                .append("studyUid", 1);
        createIndex(interpretationCollection, lockedIndex);

        logger.info("Propagating all ClinicalAnalysis 'locked' values to the related Interpretations");
        queryMongo(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, new Document(),
                Projections.include(Arrays.asList("id", "studyUid", "locked")), (doc) -> {
                    Bson query = Filters.and(
                        Filters.eq("studyUid", doc.get("studyUid")),
                        Filters.eq("clinicalAnalysisId", doc.getString("id"))
                    );
                    Bson update = Updates.set("locked", doc.getBoolean("locked"));
                    interpretationCollection.updateMany(query, update);
                });
    }
}
