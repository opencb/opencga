package org.opencb.opencga.app.migrations.v3_1_0;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Date;

@Migration(id = "addFailedLoginAttemtsMigration", description = "Add failedAttempts to User #TASK-6013", version = "3.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240419)
public class UserBanMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        String lastModified = TimeUtils.getTime();
        String expirationDate = TimeUtils.getTime(TimeUtils.add1YeartoDate(new Date()));
        migrateCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION,
                Filters.exists("internal.failedAttempts", false),
                Projections.include("_id", "internal", "account"),
                (document, bulk) -> {
                    Document internal = document.get("internal", Document.class);
                    Document account = document.get("account", Document.class);
                    internal.put("failedAttempts", 0);
                    internal.put("registrationDate", account.get("creationDate"));
                    internal.put("lastModified", lastModified);
                    account.put("expirationDate", expirationDate);

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")),
                            Updates.combine(
                                    Updates.set("internal", internal),
                                    Updates.set("account", account))
                            )
                    );
                });
    }

}
