package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;


import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

@Migration(id = "job_model_changes_6445",
        description = "Job data model changes #TASK-6445", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241113)
public class JobModelChangesMigrationTask6445 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.exists("type", false);
        Bson update = Updates.combine(
                Updates.set("type", "NATIVE"),
                Updates.set("tool.minimumRequirements", new Document()),
                Updates.set("execution.dependencies", Collections.emptyList())
        );
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION)) {
            getMongoCollection(collection).updateMany(query, update);
        }
    }

}
