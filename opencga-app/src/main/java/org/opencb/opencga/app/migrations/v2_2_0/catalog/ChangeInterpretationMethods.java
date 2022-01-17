package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "change_interpretation_method",
        description = "Remove list of methods from Interpretations #1841", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211112)
public class ChangeInterpretationMethods extends MigrationTool {

    private void updateClinicalVariant(List<Document> findings, Object filters) {
        if (findings == null) {
            return;
        }

        for (Document finding : findings) {
            finding.remove("interpretationMethodNames");
            if (filters != null) {
                finding.put("filters", filters);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                new Document(),
                Projections.include("_id", "methods", InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(),
                        InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key()),
                (interpretationDoc, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    List<Document> methods = interpretationDoc.getList("methods", Document.class);

                    Object filters = null;
                    if (methods != null) {
                        updateDocument.getUnset().add("methods");

                        Document methodDocument;
                        if (methods.size() > 0) {
                            // Edit method
                            methodDocument = methods.get(0);
                            filters = methodDocument.get("filters");
                            methodDocument.remove("filters");
                            methodDocument.remove("panels");
                        } else {
                            // Initialise
                            methodDocument = new Document()
                                    .append("name", "")
                                    .append("dependencies", Collections.emptyList());
                        }
                        methodDocument.put("version", "");
                        methodDocument.put("commit", "");

                        updateDocument.getSet().put(InterpretationDBAdaptor.QueryParams.METHOD.key(), methodDocument);
                    }

                    List<Document> primaryFindings = interpretationDoc.getList(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), Document.class);
                    if (primaryFindings != null) {
                        updateClinicalVariant(primaryFindings, filters);
                        updateDocument.getSet().put(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), primaryFindings);
                    }
                    List<Document> secondaryFindings = interpretationDoc.getList(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), Document.class);
                    if (secondaryFindings != null) {
                        updateClinicalVariant(secondaryFindings, filters);
                        updateDocument.getSet().put(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), secondaryFindings);
                    }

                    Document update = updateDocument.toFinalUpdateDocument();
                    if (!update.isEmpty()) {
                        bulk.add(new UpdateOneModel<>(
                                eq("_id", interpretationDoc.get("_id")),
                                update));
                    }
                });
    }
}
