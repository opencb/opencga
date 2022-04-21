package org.opencb.opencga.app.migrations.v2_2_1.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.StudyConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_missing_biotypes",
        description = "Add missing biotypes, #TASK-625", version = "2.2.1",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220412)
public class AddMissingBiotypes extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StudyConverter converter = new StudyConverter();
        List<String> newBiotypes = Arrays.asList("vault_RNA", "guide_RNA", "lncRNA", "IG_LV_gene", "Mt_tRNA_pseudogene", "artifact",
                "disrupted_domain");
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document(),
                Projections.include("_id", "variableSets"),
                (doc, bulk) -> {
                    Study study = converter.convertToDataModelType(doc);
                    for (VariableSet variableSet : study.getVariableSets()) {
                        if ("opencga_sample_variant_stats".equals(variableSet.getId())
                                || "opencga_cohort_variant_stats".equals(variableSet.getId())) {
                            for (Variable variable : variableSet.getVariables()) {
                                if ("biotypeCount".equals(variable.getId())) {
                                    List<String> allowedKeys = new ArrayList<>(variable.getAllowedKeys());
                                    for (String newBiotype : newBiotypes) {
                                        if (!allowedKeys.contains(newBiotype)) {
                                            allowedKeys.add(newBiotype);
                                            variable.setAllowedKeys(allowedKeys);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    List<Document> variableSetDocumentList = convertToDocument(study.getVariableSets());
                    bulk.add(new UpdateOneModel<>(
                            eq("_id", doc.get("_id")),
                            new Document("$set", new Document("variableSets", variableSetDocumentList)))
                    );
                }
        );
    }
}
