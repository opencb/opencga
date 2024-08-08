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
import java.util.Collections;

import static org.opencb.opencga.catalog.db.mongodb.UserMongoDBAdaptor.*;

@Migration(id = "add_archivePasswords_array",
        description = "Add password history #6494", version = "3.2.1",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240723)
public class AddPasswordHistoryMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists(PRIVATE_PASSWORD_ARCHIVE, false);
        Bson projection = Projections.include(PRIVATE_PASSWORD);
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.USER_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_USER_COLLECTION),
                query, projection, (document, bulk) -> {
                    String currentPassword = document.getString("_password");

                    Document passwordDoc = new Document()
                            .append(HASH, currentPassword)
                            .append(SALT, "");
                    Document privatePassword = new Document();
                    privatePassword.put(CURRENT, passwordDoc);
                    privatePassword.put(ARCHIVE, Collections.singletonList(passwordDoc));

                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    updateDocument.getSet().put(PRIVATE_PASSWORD, privatePassword);

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }

}
