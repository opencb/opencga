package org.opencb.opencga.app.migrations.v3.v3_1_0;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;

@Migration(id = "addFailedLoginAttemptsMigration", description = "Add failedAttempts to User #TASK-6013", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240419, patch = 2)
public class UserBanMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        String lastModified = TimeUtils.getTime();
        migrateCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION,
                Filters.exists("internal.registrationDate", false),
                Projections.include("_id", "internal", "account"),
                (document, bulk) -> {
                    Document internal = document.get("internal", Document.class);
                    Document account = document.get("account", Document.class);
                    internal.put("failedAttempts", 0);
                    internal.put("registrationDate", account.get("creationDate"));
                    internal.put("lastModified", lastModified);
                    account.put("expirationDate", Constants.DEFAULT_USER_EXPIRATION_DATE);

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")),
                            Updates.combine(
                                    Updates.set("internal", internal),
                                    Updates.set("account", account))
                            )
                    );
                });

        // Patch 2. Organization 'configuration.defaultUserExpirationDate' field was not set for existing organizations
        MongoCollection<Document> orgCollection = getMongoCollection(OrganizationMongoDBAdaptorFactory.ORGANIZATION_COLLECTION);
        orgCollection.updateMany(Filters.exists("configuration.defaultUserExpirationDate", false),
                Updates.set("configuration.defaultUserExpirationDate", Constants.DEFAULT_USER_EXPIRATION_DATE));
    }

}
