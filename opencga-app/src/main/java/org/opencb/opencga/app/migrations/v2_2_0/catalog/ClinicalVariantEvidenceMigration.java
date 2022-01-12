package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.Document;
import org.opencb.commons.ProgressLogger;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_clinical_variant_evidence_review",
        description = "Add new ClinicalVariantEvidenceReview object, #1874", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220112)
public class ClinicalVariantEvidenceMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION);

        int bsonMaximumSizeExceeded = 0;
        ProgressLogger progressLogger = new ProgressLogger("Execute Interpretation update").setBatchSize(100);
        try (MongoCursor<Document> it = collection
                .find(new Document("primaryFindings.evidences.review", new Document("$exists", false)))
                .projection(Projections.include("_id", "primaryFindings", "secondaryFindings")).cursor()) {
            while (it.hasNext()) {
                Document doc = it.next();

                List<Document> primaryFindings = doc.getList("primaryFindings", Document.class);
                List<Document> secondaryFindings = doc.getList("secondaryFindings", Document.class);

                Document update = new Document();
                if (!primaryFindings.isEmpty()) {
                    applyChanges(primaryFindings);
                    update.put("primaryFindings", primaryFindings);
                }
                if (!secondaryFindings.isEmpty()) {
                    applyChanges(secondaryFindings);
                    update.put("secondaryFindings", secondaryFindings);
                }

                if (!update.isEmpty()) {
                    try {
                        collection.updateOne(eq("_id", doc.get("_id")), new Document("$set", update));
                        progressLogger.increment(1);
                    } catch (BsonMaximumSizeExceededException e) {
                        bsonMaximumSizeExceeded++;
                    }
                }
            }
        }

        if (bsonMaximumSizeExceeded > 0) {
            logger.warn("{} Interpretations could not be updated because of the size of the document", bsonMaximumSizeExceeded);
        }
    }

    private void applyChanges(List<Document> clinicalVariants) {
        if (clinicalVariants.isEmpty()) {
            return;
        }

        for (Document clinicalVariant : clinicalVariants) {
            List<Document> evidences = clinicalVariant.getList("evidences", Document.class);
            if (evidences.isEmpty()) {
                continue;
            }
            for (Document evidence : evidences) {
                Document review = evidence.get("review", Document.class);
                // review field should not exist
                if (review == null) {
                    String justification = evidence.getString("justification");
                    evidence.remove("justification");

                    Document clinicalEvidenceReview = new Document()
                            .append("select", false)
                            .append("acmg", Collections.emptyList());
                    if (StringUtils.isNotEmpty(justification)) {
                        clinicalEvidenceReview.put("discussion", justification);
                    }
                    evidence.put("review", clinicalEvidenceReview);
                }
            }
        }
    }
}
