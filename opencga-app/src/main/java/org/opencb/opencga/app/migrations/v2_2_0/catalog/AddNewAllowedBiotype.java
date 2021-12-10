package org.opencb.opencga.app.migrations.v2_2_0.catalog;

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
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_new_allowed_biotype",
        description = "Add new allowed biotype 'guide_RNA'", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211209)
public class AddNewAllowedBiotype extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StudyConverter converter = new StudyConverter();
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document(),
                Projections.include("_id", "variableSets"),
                (doc, bulk) -> {
                    Study study = converter.convertToDataModelType(doc);
                    for (VariableSet variableSet : study.getVariableSets()) {
                        if ("opencga_sample_variant_stats".equals(variableSet.getId())) {
                            for (Variable variable : variableSet.getVariables()) {
                                if ("biotypeCount".equals(variable.getId())) {
                                    List<String> allowedKeys = new ArrayList<>(variable.getAllowedKeys());
                                    if (!allowedKeys.contains("guide_RNA")) {
                                        allowedKeys.add("guide_RNA");
                                        variable.setAllowedKeys(allowedKeys);
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
