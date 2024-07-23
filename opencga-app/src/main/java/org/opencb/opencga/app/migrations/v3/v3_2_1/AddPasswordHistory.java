package org.opencb.opencga.app.migrations.v3.v3_2_1;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Migration(id = "add_archivePasswords_array",
        description = "Add password history #6494", version = "3.2.1",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240723)
public class AddPasswordHistory extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("_archivePasswords", false);
        Bson projection = Projections.include("_password");
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.USER_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_USER_COLLECTION),
                query, projection, (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

                    // Add _archivePasswords
                    String currentPassword = document.getString("_password");
                    List<String> archivePasswords = new ArrayList<>();
                    if (StringUtils.isNotEmpty(currentPassword)) {
                        archivePasswords.add(currentPassword);
                    }
                    updateDocument.getSet().put("_archivePasswords", archivePasswords);

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }

}
