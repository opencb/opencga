package org.opencb.opencga.app.migrations.v6.v6_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Collections;

@Migration(id = "federation_client_active__task_8297",
        description = "Add active=true to existing federation clients #TASK-8297", version = "6.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20260512)
public class FederationClientActiveMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        // Set active=true on all federation client entries that don't have the field
        Bson query = Filters.and(
                Filters.exists("federation.clients"),
                Filters.elemMatch("federation.clients", Filters.exists("active", false))
        );
        Bson update = Updates.set("federation.clients.$[elem].active", true);
        UpdateOptions options = new UpdateOptions()
                .arrayFilters(Collections.singletonList(Filters.exists("elem.active", false)));

        getMongoCollection(OrganizationMongoDBAdaptorFactory.ORGANIZATION_COLLECTION).updateMany(query, update, options);
    }
}
