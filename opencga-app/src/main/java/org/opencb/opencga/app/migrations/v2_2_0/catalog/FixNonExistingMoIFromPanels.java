package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "fix_non_existing_mois_from_panels",
        description = "Remove non-existing MOIs from Panels", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 2,
        date = 20220111)
public class FixNonExistingMoIFromPanels extends MigrationTool {

    private void fixMoI(List<Document> documentList) {
        List<Document> newMoi = new ArrayList<>();

        for (Document document : documentList) {
            String modeOfInheritance = document.getString("modeOfInheritance");
            if (StringUtils.isNotEmpty(modeOfInheritance)) {
                switch (modeOfInheritance) {
                    case "AUTOSOMAL_DOMINANT_AND_RECESSIVE":
                    case "AUTOSOMAL_DOMINANT_AND_MORE_SEVERE_RECESSIVE":
                        // Make this moi dominant
                        document.put("modeOfInheritance", "AUTOSOMAL_DOMINANT");

                        // And generate a new one being recessive
                        Document newDoc = new Document(document);
                        newDoc.put("modeOfInheritance", "AUTOSOMAL_RECESSIVE");
                        newMoi.add(newDoc);
                        break;
                    case "AUTOSOMAL_DOMINANT_MATERNALLY_IMPRINTED":
                    case "AUTOSOMAL_DOMINANT_NOT_IMPRINTED":
                    case "AUTOSOMAL_DOMINANT_PATERNALLY_IMPRINTED":
                        document.put("modeOfInheritance", "AUTOSOMAL_DOMINANT");
                        break;

                    // Valid MOI terms
                    case "AUTOSOMAL_DOMINANT":
                    case "AUTOSOMAL_RECESSIVE":
                    case "X_LINKED_DOMINANT":
                    case "X_LINKED_RECESSIVE":
                    case "Y_LINKED":
                    case "MITOCHONDRIAL":
                    case "DE_NOVO":
                    case "MENDELIAN_ERROR":
                    case "COMPOUND_HETEROZYGOUS":
                    case "UNKNOWN":
                        break;

                    default:
                        logger.warn("Unexpected MOI term '{}' found. Setting it to unknown.", modeOfInheritance);
                        document.put("modeOfInheritance", "UNKNOWN");
                        break;
                }
            }
        }

        documentList.addAll(newMoi);
    }

    private List<Document> getDocumentList(Document document, String field) {
        List<Document> list = document.getList(field, Document.class);
        if (list == null) {
            return Collections.emptyList();
        } else {
            return new ArrayList<>(list);
        }
    }

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.PANEL_COLLECTION,
                new Document(),
                Projections.include("_id", "variants", "genes", "strs", "regions"),
                (panelDoc, bulk) -> {
                    if (panelDoc != null) {
                        List<Document> genes = getDocumentList(panelDoc, "genes");
                        fixMoI(genes);
                        List<Document> variants = getDocumentList(panelDoc, "variants");
                        fixMoI(variants);
                        List<Document> strs = getDocumentList(panelDoc, "strs");
                        fixMoI(strs);
                        List<Document> regions = getDocumentList(panelDoc, "regions");
                        fixMoI(regions);

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", panelDoc.get("_id")),
                                new Document("$set", new Document()
                                        .append("genes", genes)
                                        .append("variants", variants)
                                        .append("strs", strs)
                                        .append("regions", regions)
                                )));
                    }
                }
        );
    }

}
