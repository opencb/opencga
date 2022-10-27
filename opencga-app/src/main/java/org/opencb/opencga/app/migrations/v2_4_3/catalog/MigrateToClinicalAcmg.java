package org.opencb.opencga.app.migrations.v2_4_3.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "migrate_to_clinical_acmg_TASK-1194",
        description = "Migrate to ClinicalAcmg #TASK-1194", version = "2.4.3",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220809)
public class MigrateToClinicalAcmg extends MigrationTool {

    private void migrateAcmg(Document document, String date, String author) {
        if (document == null) {
            return;
        }

        List<String> acmg;
        try {
            acmg = document.getList("acmg", String.class);
        } catch (ClassCastException e) {
            // We try to cast it to Document to validate it is already a list of ClinicalAcmg
            document.getList("acmg", Document.class);
            logger.warn("Skipping acmg list. It seems it was already migrated: {}", e.getMessage());
            return;
        }

        if (acmg != null) {
            List<Document> acmgList = new ArrayList<>(acmg.size());
            for (String acmgValue : acmg) {
                ClinicalAcmg clinicalAcmg = new ClinicalAcmg(acmgValue, "",
                        "Values author and date automatically filled by migration script", author, date);
                Document acmgDoc = convertToDocument(clinicalAcmg);
                acmgList.add(acmgDoc);
            }
            document.put("acmg", acmgList);
        }
    }

    private void processFindings(Document interpretation, String field, Document update, String date, String author) {
        List<Document> findings = interpretation.getList(field, Document.class);
        if (findings == null) {
            return;
        }

        for (Document finding : findings) {
            List<Document> evidences = finding.getList("evidences", Document.class);
            if (evidences != null) {
                for (Document evidence : evidences) {
                    Document classification = evidence.get("classification", Document.class);
                    migrateAcmg(classification, date, author);

                    Document review = evidence.get("review", Document.class);
                    migrateAcmg(review, date, author);
                }
            }
        }

        update.put(field, findings);
    }

    private String extractAuthor(Document document) {
        String author = "";
        Document analyst = document.get("analyst", Document.class);
        if (analyst != null) {
            String id = analyst.getString("id");
            if (StringUtils.isNotEmpty(id)) {
                author = id;
            }
        }
        return author;
    }

    @Override
    protected void run() throws Exception {
        // Replace acmg for ClinicalAcmg
        /*
        findings.evidences.

classification.acmg
review.acmg
         */

        for (String collectionString : Arrays.asList(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                MongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION, MongoDBAdaptorFactory.DELETED_INTERPRETATION_COLLECTION)) {
            logger.info("Migrating documents from collection '{}'", collectionString);
            MongoCollection<Document> collection = getMongoCollection(collectionString);
            queryMongo(collectionString,
                    new Document(),
                    Projections.include("primaryFindings", "secondaryFindings", "modificationDate", "analyst", "studyUid", "id"),
                    (document) -> {
                        String date = document.getString("modificationDate");
                        String author = extractAuthor(document);

                        Document update = new Document();
                        processFindings(document, "primaryFindings", update, date, author);
                        processFindings(document, "secondaryFindings", update, date, author);

                        if (!update.isEmpty()) {
                            try {
                                collection.updateOne(eq("_id", document.get("_id")),
                                        new Document("$set", update));
                            } catch (Exception e) {
                                logger.warn("Could not replace Acmg for ClinicalAcmg to Interpretation '{}' from study uid {}. "
                                                + "Error message: {}", document.getString("id"), document.get("studyUid", Number.class),
                                        e.getMessage());
                            }
                        }
                    });
        }
    }

}
