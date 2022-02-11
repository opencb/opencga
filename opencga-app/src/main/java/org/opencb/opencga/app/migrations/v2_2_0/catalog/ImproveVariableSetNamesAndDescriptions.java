package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.StudyConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.VariableSet;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "improveVariableSetNamesAndDescriptions",
        description = "Improve VariableSet names and descriptions", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211210)
public class ImproveVariableSetNamesAndDescriptions extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Read default VariableSets
        Map<String, VariableSet> variableSetMap = new HashMap<>();
        Set<String> variablesets = new Reflections(new ResourcesScanner(), "variablesets/").getResources(Pattern.compile(".*\\.json"));
        for (String variableSetFile : variablesets) {
            VariableSet vs;
            try {
                vs = JacksonUtils.getDefaultNonNullObjectMapper().readValue(
                        getClass().getClassLoader().getResourceAsStream(variableSetFile), VariableSet.class);
            } catch (IOException e) {
                logger.error("Could not parse variable set '{}'", variableSetFile, e);
                continue;
            }
            if (vs != null) {
                variableSetMap.put(vs.getId(), vs);
            }
        }
        if (variableSetMap.isEmpty()) {
            throw new RuntimeException("Java reflection didn't work");
        }

        StudyConverter converter = new StudyConverter();
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document(),
                Projections.include("_id", "variableSets"),
                (doc, bulk) -> {
                    Study study = converter.convertToDataModelType(doc);
                    for (VariableSet variableSet : study.getVariableSets()) {
                        if (variableSetMap.containsKey(variableSet.getId())) {
                            // Change names and descriptions
                            VariableSet vs = variableSetMap.get(variableSet.getId());
                            variableSet.setName(vs.getName());
                            variableSet.setDescription(vs.getDescription());
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
