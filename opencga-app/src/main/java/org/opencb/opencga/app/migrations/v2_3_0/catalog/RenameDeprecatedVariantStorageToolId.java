package org.opencb.opencga.app.migrations.v2_3_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "rename_variant_storage_tool_ids_TASK-705",
        description = "Rename variant storage tool ids #TASK-705", version = "2.3.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20220506)
public class RenameDeprecatedVariantStorageToolId extends MigrationTool {

    @Override
    protected void run() throws Exception {
        renameToolId("variant-sample-index", " variant-secondary-sample-index");
        renameToolId("variant-secondary-index", "variant-secondary-annotation-index");
    }

    private void renameToolId(String oldToolId, String newToolId) {
        MongoCollection<Document> collection = getMongoCollection(MongoDBAdaptorFactory.JOB_COLLECTION);
        Bson query = Filters.eq("tool.id", oldToolId);
        Bson update = Updates.set("tool.id", newToolId);
        collection.updateMany(query, update);
    }
}
