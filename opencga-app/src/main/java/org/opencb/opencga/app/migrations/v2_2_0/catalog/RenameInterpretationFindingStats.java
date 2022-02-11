package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "rename_interpretation_stats_field",
        description = "Rename interpretation stats field #1819", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211115)
public class RenameInterpretationFindingStats extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.INTERPRETATION_COLLECTION,
                new Document(),
                Projections.include("_id", InterpretationDBAdaptor.QueryParams.STATS.key()),
                (interpretationDoc, bulk) -> {
                    Document stats = interpretationDoc.get("stats", Document.class);
                    if (stats == null) {
                        return;
                    }
                    Document primaryFindingsStats = stats.get("primaryFindings", Document.class);
                    if (primaryFindingsStats != null) {
                        Object statusCount = primaryFindingsStats.get("variantStatusCount");
                        primaryFindingsStats.remove("variantStatusCount");
                        if (statusCount != null) {
                            primaryFindingsStats.put("statusCount", statusCount);
                        }
                    }
                    Document secondaryFindingsStats = stats.get("secondaryFindings", Document.class);
                    if (secondaryFindingsStats != null) {
                        Object statusCount = secondaryFindingsStats.get("variantStatusCount");
                        secondaryFindingsStats.remove("variantStatusCount");
                        if (statusCount != null) {
                            secondaryFindingsStats.put("statusCount", statusCount);
                        }
                    }

                    bulk.add(new UpdateOneModel<>(
                                    eq("_id", interpretationDoc.get("_id")),
                                    new Document("$set", new Document("stats", stats))
                            )
                    );
                }
        );
    }

}
