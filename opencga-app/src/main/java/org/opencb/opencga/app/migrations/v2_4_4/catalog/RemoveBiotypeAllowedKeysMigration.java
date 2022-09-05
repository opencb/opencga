package org.opencb.opencga.app.migrations.v2_4_4.catalog;

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

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "remove_biotype_allowed_keys_TASK-1849",
        description = "Remove biotype and consequence type allowed keys from variable sets #TASK-1849", version = "2.4.4",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220905)
public class RemoveBiotypeAllowedKeysMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StudyConverter converter = new StudyConverter();
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document(),
                Projections.include("_id", "variableSets"),
                (doc, bulk) -> {
                    Study study = converter.convertToDataModelType(doc);
                    for (VariableSet variableSet : study.getVariableSets()) {
                        if ("opencga_sample_variant_stats".equals(variableSet.getId())
                                || "opencga_cohort_variant_stats".equals(variableSet.getId())) {
                            for (Variable variable : variableSet.getVariables()) {
                                if ("biotypeCount".equals(variable.getId()) || "consequenceTypeCount".equals(variable.getId())) {
                                    variable.setAllowedKeys(null);
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
