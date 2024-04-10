package org.opencb.opencga.app.migrations.v3_1_0;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.PasswordUtils;

import java.util.List;

@Migration(id = "hide_secret_key", description = "Hide secret key #TASK-5923", version = "3.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240410)
public class AuthOriginSimplificationMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("configuration.token", false);
        Bson projection = Projections.include("_id", "configuration");
        migrateCollection(OrganizationMongoDBAdaptorFactory.ORGANIZATION_COLLECTION, query, projection, (orgDocument, bulk) -> {
            Document configuration = orgDocument.get(OrganizationDBAdaptor.QueryParams.CONFIGURATION.key(), Document.class);
            if (configuration != null) {
                String secretKey = null;
                List<Document> authenticationOrigins = configuration.getList("authenticationOrigins", Document.class);
                if (CollectionUtils.isNotEmpty(authenticationOrigins)) {
                    for (Document authenticationOrigin : authenticationOrigins) {
                        if (authenticationOrigin.getString("type").equals("OPENCGA")) {
                            secretKey = authenticationOrigin.getString("secretKey");
                            if (authenticationOrigin.getString("id").equals("internal")) {
                                authenticationOrigin.put("id", "OPENCGA");
                            }
                        }
                        authenticationOrigin.remove("secretKey");
                        authenticationOrigin.remove("algorithm");
                        authenticationOrigin.remove("expiration");
                    }
                }
                secretKey = StringUtils.isNotEmpty(secretKey) ? secretKey : PasswordUtils.getStrongRandomPassword(32);
                Document token = new Document()
                        .append("secretKey", secretKey)
                        .append("algorithm", "HS256")
                        .append("expiration", 3600L);
                configuration.put("token", token);
                bulk.add(new UpdateOneModel<>(
                        Filters.eq("_id", orgDocument.get("_id")),
                        Updates.set("configuration", configuration)));
            }
        });

        // Change authOrigin id from all "internal" users
        getMongoCollection(OrganizationMongoDBAdaptorFactory.USER_COLLECTION)
                .updateMany(
                        new Document("account.authentication.id", "internal"),
                        Updates.set("account.authentication.id", "OPENCGA"));
    }

}
