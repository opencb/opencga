package org.opencb.opencga.app.migrations.v2_2_0.catalog;


import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id="rename_variableset_field",
        description = "Rename Variable variableSet field to variables #1823", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        rank = 6)
public class renameVariableSetFieldFromVariable extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document("variableSets.variables.variableSet", new Document("$exists", true)),
                Projections.include("_id", StudyDBAdaptor.QueryParams.VARIABLE_SET.key()),
                (studyDoc, bulk) -> {
                    List<Document> variablesets = studyDoc.getList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), Document.class);
                    if (variablesets != null) {
                        for (Document variableset : variablesets) {
                            List<Document> variables = variableset.getList("variables", Document.class);
                            if (variables != null) {
                                for (Document variable : variables) {
                                    renameVariableSet(variable);
                                }
                            }
                        }

                        bulk.add(new UpdateOneModel<>(
                                        eq("_id", studyDoc.get("_id")),
                                        new Document("$set", new Document(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), variablesets))
                                )
                        );
                    }
                }
        );
    }

    public void renameVariableSet(Document variable) {
        List<Document> variableList = variable.getList("variableSet", Document.class);
        if (variableList != null) {
            for (Document nestedVariable : variableList) {
                renameVariableSet(nestedVariable);
            }
        }
        variable.put("variables", variableList);
        variable.remove("variableSet");
    }

}
