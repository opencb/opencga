package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.config.ExecutionQueue;

import java.util.Arrays;

@Migration(id = "support_queues_in_job__task_5662",
        description = "Support for multiple queues in Job #7442", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250725)
public class JobQueueMigration_Task7442 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION),
                Filters.exists("tool.minimumRequirements.type", false),
                Projections.include("_id"),
                (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

                    updateDocument.getSet().put("tool.minimumRequirements.type", ExecutionQueue.ExecutionType.CPU.name());
                    updateDocument.getSet().put("tool.minimumRequirements.queue", "");
                    updateDocument.getSet().put("execution.queue", new Document());
                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }

}
