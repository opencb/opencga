package org.opencb.opencga.app.migrations.v5.v5_0_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.config.ExecutionQueue;

import java.util.Arrays;

@Migration(id = "support_queues_in_job__task_7442",
        description = "Support for multiple queues in Job #7442", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250725)
public class JobQueueMigration_Task7442 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson toolRequirementsCheck = Filters.and(
                Filters.exists("tool.minimumRequirements", true),
                Filters.ne("tool.minimumRequirements", null),
                Filters.exists("tool.minimumRequirements.processorType", false)
        );
        MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
        updateDocument.getSet().put("tool.minimumRequirements.processorType", ExecutionQueue.ProcessorType.CPU.name());
        updateDocument.getSet().put("tool.minimumRequirements.queue", "");
        Document toolRequirementsUpdate = updateDocument.toFinalUpdateDocument();

        Bson executionQueueCheck = Filters.and(
                Filters.exists("execution", true),
                Filters.exists("execution.queue", false)
        );
        updateDocument = new MongoDBAdaptor.UpdateDocument();
        updateDocument.getSet().put("execution.queue", new Document());
        Document executionQueueUpdate = updateDocument.toFinalUpdateDocument();

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION)) {
            MongoCollection<Document> mongoCollection = getMongoCollection(collection);
            mongoCollection.updateMany(toolRequirementsCheck, toolRequirementsUpdate);
            mongoCollection.updateMany(executionQueueCheck, executionQueueUpdate);
        }
    }
}
