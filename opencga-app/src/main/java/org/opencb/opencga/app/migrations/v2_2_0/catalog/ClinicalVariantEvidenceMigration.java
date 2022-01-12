package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
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

    public ClinicalVariantEvidenceMigration() {
        super(1);
    }

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                new Document(),
                Projections.include("_id", "primaryFindings", "secondaryFindings"),
                (doc, bulk) -> {
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
                        bulk.add(new UpdateOneModel<>(
                                eq("_id", doc.get("_id")),
                                new Document("$set", update))
                        );
                    }
                }
        );
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
