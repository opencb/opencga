package org.opencb.opencga.app.migrations.v2_1_0.catalog.java;

import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

@Migration(id="remove_rc_version_from_migration_runs",
        description = "Remove RC versions from migration runs stored in catalog", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        patch = 1,
        rank = 20)
public class RemoveRCVersionsFromMigrationRuns extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.MIGRATION_COLLECTION, new Document(), new Document(), (document, bulk) -> {
            String version = document.getString("version");
            if (version.toUpperCase().contains("-RC")) {
                String newVersion = version.split("-RC")[0];
                logger.info("Change version for migration " + document.getString("id") + " : " + version + " -> " + newVersion);
                bulk.add(new UpdateOneModel<>(eq("_id", document.get("_id")), set("version", newVersion)));
            }
        });
    }
}
