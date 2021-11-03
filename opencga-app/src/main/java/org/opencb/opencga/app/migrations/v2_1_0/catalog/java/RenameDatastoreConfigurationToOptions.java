package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

@Migration(id = "rename_datastore_configuration_to_options", description = "Rename project.internal.datastores.variant.configuration to options", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        date = 20210617)
public class RenameDatastoreConfigurationToOptions extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.USER_COLLECTION, new Document(), Projections.include("_id", "projects.internal", "projects.uuid"),
                (user, bulk) -> {
                    List<Document> projects = user.getList("projects", Document.class);
                    for (Document project : projects) {
                        Document configuration = get(project, "internal", "datastores", "variant", "configuration");
                        Document options = get(project, "internal", "datastores", "variant", "options");
                        if (configuration != null)
                            // Do not overwrite options if present!
                            if (options == null) {
                                bulk.add(new UpdateOneModel<>(
                                        and(eq("_id", user.get("_id")), eq("projects.uuid", project.get("uuid"))),
                                        Updates.set("projects.$.internal.datastores.variant.options", configuration)));
                            }
                    }
                }
        );
    }

    protected Document get(Document document, String... keys) {
        for (String key : keys) {
            document = document.get(key, Document.class);
            if (document == null) {
                return null;
            }
        }
        return document;
    }
}
