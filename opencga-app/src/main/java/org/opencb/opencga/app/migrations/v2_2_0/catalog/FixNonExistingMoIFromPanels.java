package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "fix_non_existing_mois_from_panels",
        description = "Remove non-existing MOIs from Panels", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220111)
public class FixNonExistingMoIFromPanels extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.PANEL_COLLECTION,
                new Document(),
                Projections.include("_id", "genes"),
                (panelDoc, bulk) -> {
                    if (panelDoc != null) {
                        List<Document> newMoi = new ArrayList<>();
                        List<Document> genes = new ArrayList<>(panelDoc.getList("genes", Document.class));
                        for (Document gene : genes) {
                            String modeOfInheritance = gene.getString("modeOfInheritance");
                            if (StringUtils.isNotEmpty(modeOfInheritance)) {
                                switch (modeOfInheritance) {
                                    case "AUTOSOMAL_DOMINANT_AND_RECESSIVE":
                                    case "AUTOSOMAL_DOMINANT_AND_MORE_SEVERE_RECESSIVE":
                                        // Make this moi dominant
                                        gene.put("modeOfInheritance", "AUTOSOMAL_DOMINANT");

                                        // And generate a new one being recessive
                                        Document newDoc = new Document(gene);
                                        newDoc.put("modeOfInheritance", "AUTOSOMAL_RECESSIVE");
                                        newMoi.add(newDoc);
                                        break;
                                    case "AUTOSOMAL_DOMINANT_MATERNALLY_IMPRINTED":
                                    case "AUTOSOMAL_DOMINANT_NOT_IMPRINTED":
                                    case "AUTOSOMAL_DOMINANT_PATERNALLY_IMPRINTED":
                                        gene.put("modeOfInheritance", "AUTOSOMAL_DOMINANT");
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
                                        gene.put("modeOfInheritance", "UNKNOWN");
                                        break;
                                }
                            }
                        }

                        genes.addAll(newMoi);

                        bulk.add(new UpdateOneModel<>(
                                eq("_id", panelDoc.get("_id")),
                                new Document("$set", new Document("genes", genes))));

                    }
                }
        );
    }

}
