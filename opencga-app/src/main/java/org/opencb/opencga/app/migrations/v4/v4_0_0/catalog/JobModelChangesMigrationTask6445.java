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
        Bson typeQuery = Filters.exists("type", false);
        Bson typeUpdate = Updates.set("type", "NATIVE");

        Bson requirementsQuery = Filters.and(
                Filters.exists("tool.id"),
                Filters.exists("tool.minimumRequirements", false)
        );
        Bson requirementsUpdate = Updates.set("tool.minimumRequirements", new Document());

        Bson dependenciesQuery = Filters.and(
                Filters.exists("execution.executor"),
                Filters.exists("execution.dependencies", false)
        );
        Bson dependenciesUpdate = Updates.set("execution.dependencies", Collections.emptyList());
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION)) {
            getMongoCollection(collection).updateMany(typeQuery, typeUpdate);
            getMongoCollection(collection).updateMany(requirementsQuery, requirementsUpdate);
            getMongoCollection(collection).updateMany(dependenciesQuery, dependenciesUpdate);
        }
    }

}
