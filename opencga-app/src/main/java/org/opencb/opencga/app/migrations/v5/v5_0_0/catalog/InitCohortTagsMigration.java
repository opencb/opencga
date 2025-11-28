package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

@Migration(id = "initCohortTags__task_7902",
        description = "Init Cohort Tags #7902", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250904)

public class InitCohortTagsMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("tags", false);
        Bson update = Updates.set("tags", Collections.emptyList());

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_COHORT_COLLECTION)) {
            getMongoCollection(collection).updateMany(query, update);
        }

        // Generate new indexes
        catalogManager.installIndexes(organizationId, token);
    }

}
