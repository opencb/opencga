package org.opencb.opencga.app.migrations.v2_2_0.catalog.issue_1849;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "complete_user_status_models",
        description = "Complete User Status data models #1849", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211126)
public class CompleteUserStatusDataModel extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.USER_COLLECTION,
                new Document("internal.status.id", new Document("$exists", false)),
                Projections.include("_id", "internal"),
                (doc, bulk) -> {
                    CompleteStatusDataModelUtils.completeInternalStatus(doc);

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", doc.get("_id")),
                            new Document("$set", new Document()
                                    .append("internal", doc.get("internal"))
                            ))
                    );
                });
    }
}
