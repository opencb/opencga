package org.opencb.opencga.app.migrations.v5.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "external_tool__task_7610",
        description = "Extend Workflow data model for new ExternalTool, #TASK-7610", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250506)
public class ExternalToolTask7610Migration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("manager");
        Bson projection = new Document();
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.DEPRECATED_WORKFLOW_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DEPRECATED_WORKFLOW_ARCHIVE_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DEPRECATED_DELETED_WORKFLOW_COLLECTION), query, projection, (document, bulk) -> {
            MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

            updateDocument.getSet().put("scope", document.get("type"));
            updateDocument.getSet().put("type", "WORKFLOW");
            Document repository = document.get("repository", Document.class);
            if (repository != null) {
                repository.put("name", repository.get("id"));
                repository.put("tag", repository.get("version"));
                repository.put("user", "");
                repository.put("password", "");
                repository.remove("id");
                repository.remove("version");
            }
            updateDocument.getSet().put("workflow", new Document()
                    .append("manager", document.get("manager"))
                    .append("repository", repository)
                    .append("scripts", document.get("scripts"))
            );
            updateDocument.getUnset().add("manager");
            updateDocument.getUnset().add("repository");
            updateDocument.getUnset().add("scripts");
            updateDocument.getSet().put("docker", null);

            bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
        });
    }

}
