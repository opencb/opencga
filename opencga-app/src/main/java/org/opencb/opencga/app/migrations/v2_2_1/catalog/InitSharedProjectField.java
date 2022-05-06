package org.opencb.opencga.app.migrations.v2_2_1.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;

@Migration(id = "init_shared_project",
        description = "Initialise sharedProjects #TASK-702", version = "2.2.1",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220502)
public class InitSharedProjectField extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.USER_COLLECTION);
        Bson query = Filters.or(
            Filters.exists("sharedProjects", false),
            Filters.eq("sharedProjects", null)
        );
        Bson update = Updates.set("sharedProjects", Collections.emptyList());

        collection.updateMany(query, update);
    }
}
