package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "add_panel_lock",
        description = "Add new panelLock to ClinicalAnalysis #1802", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210713)
public class PanelLock extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                new Document(ClinicalAnalysisDBAdaptor.QueryParams.PANEL_LOCK.key(), new Document("$exists", false)),
                Projections.include("_id"),
                (clinical, bulk) -> {
                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", clinical.get("_id")),
                                    new Document("$set", new Document(ClinicalAnalysisDBAdaptor.QueryParams.PANEL_LOCK.key(), false))
                            )
                    );
                }
        );
    }
}
