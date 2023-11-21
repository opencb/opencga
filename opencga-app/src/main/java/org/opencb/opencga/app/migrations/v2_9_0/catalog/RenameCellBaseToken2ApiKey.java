package org.opencb.opencga.app.migrations.v2_9_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "rename_cellbase_token_2_api_key" ,
        description = "Rename CellBase Token to ApiKey",
        version = "2.9.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230829
)
public class RenameCellBaseToken2ApiKey extends MigrationTool {

    @Override
    protected void run() throws Exception {

        migrateCollection(MongoDBAdaptorFactory.USER_COLLECTION,
                new Document("projects.id", new Document("$exists", true)),
                Projections.include("_id", "projects"),
                (userDocument, bulk) -> {
                    List<Document> projects = userDocument.getList("projects", Document.class);
                    for (int i = 0; i < projects.size(); i++) {
                        Document project = projects.get(i);
                        Document cellbase = project.get("cellbase", Document.class);
                        if (cellbase != null) {
                            String token = cellbase.getString("token");
                            if (token != null) {
                                bulk.add(new UpdateOneModel<>(
                                        eq("_id", userDocument.get("_id")),
                                        Updates.combine(
                                                Updates.set("projects." + i + ".cellbase.apiKey", token),
                                                Updates.unset("projects." + i + ".cellbase.token")
                                        )));
                            }
                        }
                    }
                });
    }

}
