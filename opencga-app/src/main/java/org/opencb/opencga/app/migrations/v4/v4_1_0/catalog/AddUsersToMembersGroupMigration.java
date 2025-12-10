package org.opencb.opencga.app.migrations.v4.v4_1_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.config.UserOrganizationConfiguration;

@Migration(id = "addUsersToMembersOrgConfiguration__task_7545",
        description = "Add addToStudyMembers #7545", version = "4.1.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250402,
        deprecatedSince = "5.0.0")
public class AddUsersToMembersGroupMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("configuration.user", false);
        Bson projection = Projections.include("_id", "configuration");
        migrateCollection(OrganizationMongoDBAdaptorFactory.ORGANIZATION_COLLECTION,
                query, projection, (document, bulk) -> {
                    Document configuration = document.get("configuration", Document.class);
                    String defaultExpiration = configuration.getString("defaultUserExpirationDate");
                    if (StringUtils.isEmpty(defaultExpiration)) {
                        defaultExpiration = Constants.DEFAULT_USER_EXPIRATION_DATE;
                    }
                    UserOrganizationConfiguration userOrganizationConfiguration = new UserOrganizationConfiguration(defaultExpiration, false);
                    Document userConfig = convertToDocument(userOrganizationConfiguration);

                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    updateDocument.getUnset().add("configuration.defaultUserExpirationDate");
                    updateDocument.getSet().put("configuration.user", userConfig);

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }

}