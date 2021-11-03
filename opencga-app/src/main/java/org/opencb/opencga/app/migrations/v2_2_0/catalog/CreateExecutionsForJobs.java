package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.ExecutionMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.JobMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.ExecutionConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.ExecutionInternal;
import org.opencb.opencga.core.models.job.Job;

import java.util.Collections;
import java.util.List;

@Migration(id = "create_executions_for_jobs",
        description = "Create Pipelines and Executions #1805", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211103)
public class CreateExecutionsForJobs extends MigrationTool {

    private ExecutionConverter executionConverter;
    private JobConverter jobConverter;
    private ExecutionMongoDBAdaptor executionDBAdaptor;
    private JobMongoDBAdaptor jobDBAdaptor;

    private Execution generateExecution(Job job) {
        ExecutionInternal internal = ExecutionInternal.init()
                .setToolId(job.getTool().getId())
                .setStatus(job.getInternal().getStatus());
        return new Execution(job.getStudyUid(), job.getId(), null, job.getDescription(), job.getUserId(),
                job.getCreationDate(), job.getModificationDate(), job.getParams(), job.getPriority(), internal, job.getOutDir(),
                job.getTags(), Collections.emptyList(), null, null, null, false, true, Collections.emptyList(), job.getRelease(),
                job.getStudy(), job.getAttributes());
    }

    private void addJobToExecution(ClientSession clientSession, long executionUid, Job job) throws CatalogDBException {
        List<Job> jobList = Collections.singletonList(job);
        List<Document> documentList = executionConverter.convertExecutionsOrJobsToDocument(jobList);
        Bson bsonQuery = Filters.eq(ExecutionDBAdaptor.QueryParams.UID.key(), executionUid);

        Bson update = Updates.addEachToSet(ExecutionDBAdaptor.QueryParams.JOBS.key(), documentList);
        DataResult<?> result = executionDBAdaptor.getExecutionCollection().update(clientSession, bsonQuery, update, QueryOptions.empty());

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Could not update list of jobs. Execution uid '" + executionUid + "' not found.");
        }
    }

    private void addExecutionReferenceToJob(ClientSession clientSession, long jobUid, String executionId) throws CatalogDBException {
        Bson bsonQuery = Filters.eq(JobDBAdaptor.QueryParams.UID.key(), jobUid);
        Bson update = Updates.set(JobDBAdaptor.QueryParams.EXECUTION_ID.key(), executionId);
        DataResult<?> result = jobDBAdaptor.getJobCollection().update(clientSession, bsonQuery, update, QueryOptions.empty());

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Could not update executionId field of job uid '" + jobUid + "'.");
        }
    }

    @Override
    protected void run() throws Exception {
        this.executionConverter = new ExecutionConverter();
        this.jobConverter = new JobConverter();
        this.executionDBAdaptor = dbAdaptorFactory.getCatalogExecutionDBAdaptor();
        this.jobDBAdaptor = dbAdaptorFactory.getCatalogJobDBAdaptor();

        // Create missing indexes (creates indexes for new Execution and Pipeline collections)
        catalogManager.installIndexes(token);

        // Create 1 Execution for every single job
        int numExecutions = 0;
        try (MongoCursor<Document> it = getMongoCollection(MongoDBAdaptorFactory.JOB_COLLECTION)
                .find(new Document(JobDBAdaptor.QueryParams.EXECUTION_ID.key(), new Document("$exists", false)))
                .cursor()) {
            while (it.hasNext()) {
                Document jobDoc = it.next();
                runTransaction(clientSession -> {
                    Job job = jobConverter.convertToDataModelType(jobDoc);
                    Execution execution = generateExecution(job);

                    // Insert execution with NO jobs
                    executionDBAdaptor.insert(clientSession, execution.getStudyUid(), execution);

                    // Update execution to contain the job
                    addJobToExecution(clientSession, execution.getUid(), job);

                    // Update job to have the executionId reference
                    addExecutionReferenceToJob(clientSession, job.getUid(), execution.getId());

                    return true;
                });
                numExecutions++;
            }
        }

        if (numExecutions == 0) {
            logger.info("Nothing to do!");
        } else {
            logger.info("Added {} new Execution documents", numExecutions);
        }
    }


}
