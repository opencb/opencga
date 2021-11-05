package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.*;
import org.opencb.opencga.catalog.db.mongodb.converters.ExecutionConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.ExecutionInternal;
import org.opencb.opencga.core.models.job.Job;

import java.util.ArrayList;
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

    private void addJobToExecution(Job job, MongoDBAdaptor.UpdateDocument updateDocument) {
        List<Job> jobList = Collections.singletonList(job);
        List<Document> documentList = executionConverter.convertExecutionsOrJobsToDocument(jobList);
        updateDocument.getAddToSet().put(ExecutionDBAdaptor.QueryParams.JOBS.key(), documentList);
    }

    private void replicatePermissions(Document jobDoc, MongoDBAdaptor.UpdateDocument updateDocument) {
        updateDocument.getSet().put(MongoDBAdaptor.PERMISSION_RULES_APPLIED, jobDoc.get(MongoDBAdaptor.PERMISSION_RULES_APPLIED));
        if (jobDoc.get(AuthorizationMongoDBAdaptor.QueryParams.ACL.key()) != null) {
            updateDocument.getSet().put(AuthorizationMongoDBAdaptor.QueryParams.ACL.key(),
                    jobDoc.get(AuthorizationMongoDBAdaptor.QueryParams.ACL.key()));
        }
        if (jobDoc.get(AuthorizationMongoDBAdaptor.QueryParams.USER_DEFINED_ACLS.key()) != null) {
            updateDocument.getSet().put(AuthorizationMongoDBAdaptor.QueryParams.USER_DEFINED_ACLS.key(),
                    jobDoc.get(AuthorizationMongoDBAdaptor.QueryParams.USER_DEFINED_ACLS.key()));
        }
    }

    private void completeExecutionDocument(ClientSession clientSession, long executionUid, Document jobDoc, Job job)
            throws CatalogDBException {
        MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();

        addJobToExecution(job, updateDocument);
        replicatePermissions(jobDoc, updateDocument);
        Document update = updateDocument.toFinalUpdateDocument();

        Bson bsonQuery = Filters.eq(ExecutionDBAdaptor.QueryParams.UID.key(), executionUid);

        DataResult<?> result = executionDBAdaptor.getExecutionCollection().update(clientSession, bsonQuery, update, QueryOptions.empty());

        if (result.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not update execution. Execution uid '" + executionUid + "' not found.");
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

    // Replaces VIEW_JOBS, WRITE_JOBS and DELETE_JOBS for VIEW_EXECUTION, WRITE_EXECUTIONS, DELETE_EXECUTIONS
    private void replaceAclPermissionString(Document studyDocument, String key, MongoDBAdaptor.UpdateDocument updateDocument) {
        List<String> aclList = studyDocument.getList(key, String.class);
        if (CollectionUtils.isNotEmpty(aclList)) {
            List<String> newList = new ArrayList<>(aclList.size());
            for (String acl : aclList) {
                String newAcl = acl.replace("__VIEW_JOBS", "__VIEW_EXECUTIONS")
                        .replace("__WRITE_JOBS", "__WRITE_EXECUTIONS")
                        .replace("__DELETE_JOBS", "__DELETE_EXECUTIONS");
                newList.add(newAcl);
            }
            updateDocument.getSet().put(key, newList);
        }
    }

    @Override
    protected void run() throws Exception {
        this.executionConverter = new ExecutionConverter();
        this.jobConverter = new JobConverter();
        this.executionDBAdaptor = dbAdaptorFactory.getCatalogExecutionDBAdaptor();
        this.jobDBAdaptor = dbAdaptorFactory.getCatalogJobDBAdaptor();

        // Change jobAcls for Execution Acls
        MongoCollection<Document> studyCollection = getMongoCollection(MongoDBAdaptorFactory.STUDY_COLLECTION);
        try (MongoCursor<Document> it = studyCollection
                .find(new Document(AuthorizationMongoDBAdaptor.QueryParams.ACL.key(), new Document("$exists", true)))
                .projection(new Document(AuthorizationMongoDBAdaptor.QueryParams.ACL.key(), 1)
                        .append(AuthorizationMongoDBAdaptor.QueryParams.USER_DEFINED_ACLS.key(), 1))
                .cursor()) {
            while (it.hasNext()) {
                Document studyDoc = it.next();
                MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                replaceAclPermissionString(studyDoc, AuthorizationMongoDBAdaptor.QueryParams.ACL.key(), updateDocument);
                replaceAclPermissionString(studyDoc, AuthorizationMongoDBAdaptor.QueryParams.USER_DEFINED_ACLS.key(), updateDocument);

                Document update = updateDocument.toFinalUpdateDocument();
                if (!update.isEmpty()) {
                    Document query = new Document("_id", studyDoc.get("_id"));
                    studyCollection.updateOne(query, update);
                }
            }
        }

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

                    // Update execution to contain job and job acls
                    completeExecutionDocument(clientSession, execution.getUid(), jobDoc, job);

                    // Update job to have the executionId reference and remove acls
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
