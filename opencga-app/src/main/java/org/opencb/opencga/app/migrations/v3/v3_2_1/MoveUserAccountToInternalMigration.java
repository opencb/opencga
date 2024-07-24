package org.opencb.opencga.app.migrations.v3.v3_2_1;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "move_user_account_to_internal",
        description = "Move account to internal.account #6494", version = "3.2.1",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240723)
public class MoveUserAccountToInternalMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("account", true);
        Bson projection = Projections.include("internal", "account");
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.USER_COLLECTION,
                        OrganizationMongoDBAdaptorFactory.DELETED_USER_COLLECTION),
                query, projection, (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

                    Document account = document.get("account", Document.class);
                    Document internal = document.get("internal", Document.class);
                    internal.put("account", account);

                    updateDocument.getSet().put("modificationDate", internal.get("lastModified"));
                    updateDocument.getSet().put("creationDate", account.get("creationDate"));
                    account.remove("creationDate");

                    Document password = new Document()
                            .append("expirationDate", null)
                            .append("lastModified", internal.get("lastModified"));
                    account.put("password", password);
                    account.put("failedAttempts", internal.get("failedAttempts"));
                    internal.remove("failedAttempts");

                    updateDocument.getSet().put("internal", internal);
                    updateDocument.getUnset().add("account");

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }
}
