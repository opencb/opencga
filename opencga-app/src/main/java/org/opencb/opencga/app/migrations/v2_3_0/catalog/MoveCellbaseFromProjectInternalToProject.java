package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "move_cellbase_from_project_internal_to_project_TASK-354",
        description = "Move cellbase from project.internal to project #TASK-354", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220422)

public class MoveCellbaseFromProjectInternalToProject extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.USER_COLLECTION,
                new Document("projects.id", new Document("$exists", true)),
                Projections.include("_id", "projects"),
                (user, bulk) -> {
                    List<Document> projects = user.getList("projects", Document.class);
                    for (Document project : projects) {
                        Document internal = project.get("internal", Document.class);
                        if (internal != null) {
                            Document cellbase = internal.get("cellbase", Document.class);
                            project.put("cellbase", cellbase);
                            internal.remove("cellbase");
                        }
                    }

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", user.get("_id")),
                            Updates.set("projects", projects)));
                }
        );
    }

}
