package org.opencb.opencga.app.migrations.v2_5_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "migrate_clinical_analyst_to_analysts-2214",
        description = "Migrate Clinical analyst to list of analysts #TASK-2214", version = "2.5.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20221017)
public class AnalystToAnalystsMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(Arrays.asList(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                        MongoDBAdaptorFactory.DELETED_CLINICAL_ANALYSIS_COLLECTION),
                Filters.exists("analysts", false),
                Projections.include("_id", "id", "studyUid", "analyst"),
                (doc, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    updateDocument.getUnset().add("analyst");
                    Document analyst = null;
                    try {
                        analyst = doc.get("analyst", Document.class);
                    } catch (Exception e) {
                        logger.warn("analyst not found in Case '{}' of study uid {}", doc.getString("id"), doc.get("studyUid"));
                    }

                    if (analyst != null) {
                        updateDocument.getSet().put("analysts", Collections.singletonList(analyst));
                    } else {
                        updateDocument.getSet().put("analysts", Collections.emptyList());
                    }

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", doc.get("_id")),
                            updateDocument.toFinalUpdateDocument())
                    );
                }
        );

    }

}
