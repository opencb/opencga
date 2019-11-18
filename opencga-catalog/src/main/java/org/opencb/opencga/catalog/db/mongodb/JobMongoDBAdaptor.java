/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.JobMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class JobMongoDBAdaptor extends MongoDBAdaptor implements JobDBAdaptor {

    private final MongoDBCollection jobCollection;
    private final MongoDBCollection deletedJobCollection;
    private JobConverter jobConverter;

    private static final String PRIVATE_PRIORITY = "_priority";

    public JobMongoDBAdaptor(MongoDBCollection jobCollection, MongoDBCollection deletedJobCollection,
                             MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.jobCollection = jobCollection;
        this.deletedJobCollection = deletedJobCollection;
        this.jobConverter = new JobConverter();
    }

    /**
     *
     * @return MongoDB connection to the job collection.
     */
    public MongoDBCollection getJobCollection() {
        return jobCollection;
    }

    public boolean exists(ClientSession clientSession, long jobUid) throws CatalogDBException {
        return count(clientSession, new Query(QueryParams.UID.key(), jobUid)).getNumMatches() > 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> job, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(job, "job");
        return new OpenCGAResult(jobCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, Job job, QueryOptions options) throws CatalogDBException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting job insert transaction for job id '{}'", job.getId());

                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                insert(clientSession, studyId, job);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create job {}: {}", job.getId(), e.getMessage());
            throw e;
        }
    }

    long insert(ClientSession clientSession, long studyId, Job job) throws CatalogDBException {
        List<Bson> filterList = new ArrayList<>();
        filterList.add(Filters.eq(QueryParams.ID.key(), job.getId()));
        filterList.add(Filters.eq(PRIVATE_STUDY_UID, studyId));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = jobCollection.count(clientSession, bson);

        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Job { id: '" + job.getId() + "'} already exists.");
        }

        long jobUid = getNewUid(clientSession);
        job.setUid(jobUid);
        job.setStudyUid(studyId);
        if (StringUtils.isEmpty(job.getUuid())) {
            job.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.JOB));
        }
        if (StringUtils.isEmpty(job.getCreationDate())) {
            job.setCreationDate(TimeUtils.getTime());
        }
        if (job.getPriority() == null) {
            job.setPriority(Enums.Priority.LOW);
        }

        Document jobObject = jobConverter.convertToStorageType(job);
        jobObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(job.getCreationDate()));
        jobObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        jobObject.put(PRIVATE_PRIORITY, job.getPriority().getValue());

        logger.debug("Inserting job '{}' ({})...", job.getId(), job.getUid());
        jobCollection.insert(clientSession, jobObject, null);
        logger.debug("Job '{}' successfully inserted", job.getId());
        return jobUid;
    }

    @Override
    public OpenCGAResult<Job> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        // Check the studyId first and throw an Exception is not found
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

        // Retrieve and return Jobs
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId);
        return get(query, options);
    }

    @Override
    public String getStatus(long jobId, String sessionId) throws CatalogDBException {   // TODO remove?
        throw new UnsupportedOperationException("Not implemented method");
    }

    @Override
    public long getStudyId(long jobId) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), jobId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_UID);
        OpenCGAResult<Document> queryResult = nativeGet(query, queryOptions);

        if (queryResult.getNumResults() != 0) {
            Object id = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return id instanceof Number ? ((Number) id).longValue() : Long.parseLong(id.toString());
        } else {
            throw CatalogDBException.uidNotFound("Job", jobId);
        }
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(jobCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return new OpenCGAResult<>(jobCollection.count(clientSession, bsonDocument));
    }


    @Override
    public OpenCGAResult<Long> count(long studyUid, Query query, String user, StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_JOBS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), studyUid);
        OpenCGAResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + studyUid + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getJobPermission().name(), Enums.Resource.JOB.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Job count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(jobCollection.count(bson));
    }

    @Override
    public OpenCGAResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return new OpenCGAResult(jobCollection.distinct(field, bsonDocument));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult update(long jobUid, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Job> dataResult = get(jobUid, options);

        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update job. Job uid '" + jobUid + "' not found.");
        }

        try {
            return runTransaction(session -> privateUpdate(session, dataResult.first(), parameters));
        } catch (CatalogDBException e) {
            logger.error("Could not update job {}: {}", dataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update job " + dataResult.first().getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single job
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one job");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Job> iterator = iterator(query, options);

        OpenCGAResult<Job> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Job job = iterator.next();
            try {
                result.append(runTransaction(session -> privateUpdate(session, job, parameters)));
            } catch (CatalogDBException e) {
                logger.error("Could not update job {}: {}", job.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, job.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Job job, ObjectMap parameters) throws CatalogDBException {
        long tmpStartTime = startQuery();

        Document jobParameters = getValidatedUpdateParams(parameters);
        if (jobParameters.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to update. Empty 'parameters' object");
        }

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), job.getStudyUid())
                .append(QueryParams.UID.key(), job.getUid());
        Bson finalQuery = parseQuery(tmpQuery);

        logger.debug("Job update: query : {}, update: {}",
                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                jobParameters.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = jobCollection.update(clientSession, finalQuery, new Document("$set", jobParameters), null);

        if (result.getNumMatches() == 0) {
            throw new CatalogDBException("Job " + job.getId() + " not found");
        }
        List<Event> events = new ArrayList<>();
        if (result.getNumUpdated() == 0) {
            events.add(new Event(Event.Type.WARNING, job.getId(), "Job was already updated"));
        }
        logger.debug("Job {} successfully updated", job.getId());

        return endWrite(tmpStartTime, 1, 1, events);
    }

    @Override
    public OpenCGAResult delete(Job job) throws CatalogDBException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), job.getUid())
                    .append(QueryParams.STUDY_UID.key(), job.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find job " + job.getId() + " with uid " + job.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete job {}: {}", job.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete job " + job.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        DBIterator<Document> iterator = nativeIterator(query, QueryOptions.empty());

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document job = iterator.next();
            String jobId = job.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, job)));
            } catch (CatalogDBException e) {
                logger.error("Could not delete job {}: {}", jobId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, jobId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document jobDocument) throws CatalogDBException {
        long tmpStartTime = startQuery();

        String jobId = jobDocument.getString(QueryParams.ID.key());
        long jobUid = jobDocument.getLong(PRIVATE_UID);
        long studyUid = jobDocument.getLong(PRIVATE_STUDY_UID);

        logger.debug("Deleting job {} ({})", jobId, jobUid);

        // Add status DELETED
        jobDocument.put(QueryParams.STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"));

        // Upsert the document into the DELETED collection
        Bson query = new Document()
                .append(QueryParams.ID.key(), jobId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedJobCollection.update(clientSession, query, new Document("$set", jobDocument),
                new QueryOptions(MongoDBCollection.UPSERT, true));

        // Delete the document from the main COHORT collection
        query = new Document()
                .append(PRIVATE_UID, jobUid)
                .append(PRIVATE_STUDY_UID, studyUid);
        DataResult remove = jobCollection.remove(clientSession, query, null);
        if (remove.getNumMatches() == 0) {
            throw new CatalogDBException("Job " + jobId + " not found");
        }
        if (remove.getNumDeleted() == 0) {
            throw new CatalogDBException("Job " + jobId + " could not be deleted");
        }
        logger.debug("Job {} successfully deleted", jobId);

        return endWrite(tmpStartTime, 1, 0, 0, 1, null);
    }

    private Document getValidatedUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Document jobParameters = new Document();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.USER_ID.key(), QueryParams.DESCRIPTION.key(),
                QueryParams.COMMAND_LINE.key(),
        };
        filterStringParams(parameters, jobParameters, acceptedParams);

        String[] acceptedStringListParams = {QueryParams.TAGS.key()};
        filterStringListParams(parameters, jobParameters, acceptedStringListParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            jobParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            jobParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.STATUS.key())) {
            if (parameters.get(QueryParams.STATUS.key()) instanceof Job.JobStatus) {
                jobParameters.put(QueryParams.STATUS.key(), getMongoDBDocument(parameters.get(QueryParams.STATUS.key()), "Job.JobStatus"));
            } else {
                jobParameters.put(QueryParams.STATUS.key(), parameters.get(QueryParams.STATUS.key()));
            }
        }

        if (parameters.containsKey(QueryParams.INPUT.key())) {
            List<Object> fileList = parameters.getList(QueryParams.INPUT.key());
            jobParameters.put(QueryParams.INPUT.key(), jobConverter.convertFilesToDocument(fileList));
        }
        if (parameters.containsKey(QueryParams.OUTPUT.key())) {
            List<Object> fileList = parameters.getList(QueryParams.OUTPUT.key());
            jobParameters.put(QueryParams.OUTPUT.key(), jobConverter.convertFilesToDocument(fileList));
        }
        if (parameters.containsKey(QueryParams.OUT_DIR.key())) {
            jobParameters.put(QueryParams.OUT_DIR.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.OUT_DIR.key())));
        }
        if (parameters.containsKey(QueryParams.TMP_DIR.key())) {
            jobParameters.put(QueryParams.TMP_DIR.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.TMP_DIR.key())));
        }
        if (parameters.containsKey(QueryParams.LOG.key())) {
            jobParameters.put(QueryParams.LOG.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.LOG.key())));
        }
        if (parameters.containsKey(QueryParams.ERROR_LOG.key())) {
            jobParameters.put(QueryParams.ERROR_LOG.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.ERROR_LOG.key())));
        }

        if (parameters.containsKey(QueryParams.PRIORITY.key())) {
            jobParameters.put(QueryParams.PRIORITY.key(), parameters.getString(QueryParams.PRIORITY.key()));
            jobParameters.put(PRIVATE_PRIORITY, Enums.Priority.getPriority(parameters.getString(QueryParams.PRIORITY.key())).getValue());
        }

        String[] acceptedObjectParams = {QueryParams.RESULT.key()};
        filterObjectParams(parameters, jobParameters, acceptedObjectParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key()};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        if (!jobParameters.isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            jobParameters.put(QueryParams.MODIFICATION_DATE.key(), time);
            jobParameters.put(PRIVATE_MODIFICATION_DATE, date);
        }

        return jobParameters;
    }

    public OpenCGAResult clean(int id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        OpenCGAResult<Job> jobDataResult = get(query, null);
        if (jobDataResult.getResults().size() == 1) {
            DataResult delete = jobCollection.remove(parseQuery(query), null);
            if (delete.getNumUpdated() == 0) {
                throw CatalogDBException.newInstance("Job id '{}' has not been deleted", id);
            }
            return new OpenCGAResult(delete);
        } else {
            throw CatalogDBException.uidNotFound("Job id '{}' does not exist (or there are too many)", id);
        }
    }


    @Override
    public OpenCGAResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public OpenCGAResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public OpenCGAResult<Job> get(long jobId, QueryOptions options) throws CatalogDBException {
        checkId(jobId);
        Query query = new Query(QueryParams.UID.key(), jobId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(jobId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Job> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Job> documentList = new ArrayList<>();
        OpenCGAResult<Job> queryResult;
        try (DBIterator<Job> dbIterator = iterator(studyUid, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            OpenCGAResult<Long> count = count(studyUid, query, user, StudyAclEntry.StudyPermissions.VIEW_JOBS);
            queryResult.setNumMatches(count.getNumMatches());
        }
        return queryResult;
    }

    @Override
    public OpenCGAResult<Job> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Job> documentList = new ArrayList<>();
        OpenCGAResult<Job> queryResult;
        try (DBIterator<Job> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            OpenCGAResult<Long> count = count(query);
            queryResult.setNumMatches(count.getNumMatches());
        }
        return queryResult;
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        OpenCGAResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            OpenCGAResult<Long> count = count(query);
            queryResult.setNumMatches(count.getNumMatches());
        }
        return queryResult;
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        OpenCGAResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(studyUid, query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            OpenCGAResult<Long> count = count(query);
            queryResult.setNumMatches(count.getNumMatches());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Job> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new JobMongoDBIterator(mongoCursor, dbAdaptorFactory.getCatalogFileDBAdaptor(), jobConverter, null);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new JobMongoDBIterator(mongoCursor, dbAdaptorFactory.getCatalogFileDBAdaptor(), null, null);
    }

    @Override
    public DBIterator<Job> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, studyUid);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        return new JobMongoDBIterator(mongoCursor, dbAdaptorFactory.getCatalogFileDBAdaptor(), jobConverter, null, user, studyUid);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(null, studyUid);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, studyDocument, user);
        return new JobMongoDBIterator(mongoCursor, dbAdaptorFactory.getCatalogFileDBAdaptor(), null, null, user, studyUid);
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options, Document studyDocument, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Enums.Resource.JOB.name());
        }

        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = fixOptions(qOptions);

        logger.debug("Job get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return jobCollection.nativeQuery().find(bson, qOptions).iterator();
        } else {
            return deletedJobCollection.nativeQuery().find(bson, qOptions).iterator();
        }
    }

    private QueryOptions fixOptions(QueryOptions queryOptions) {
        QueryOptions options = new QueryOptions(queryOptions);

        filterOptions(options, FILTER_ROUTE_JOBS);
        if (options.containsKey(QueryOptions.SORT)) {
            // If the user is sorting by priority, we will point to the private priority stored as integers to properly sort
            List<String> sortList = options.getAsStringList(QueryOptions.SORT);
            List<String> fixedSortList = new ArrayList<>(sortList.size());
            for (String key : sortList) {
                if (key.startsWith(QueryParams.PRIORITY.key())) {
                    String[] priorityArray = key.split(":");
                    if (priorityArray.length == 1) {
                        fixedSortList.add(PRIVATE_PRIORITY);
                    } else {
                        // The order (ascending or descending) should be in priorityArray[1]
                        fixedSortList.add(PRIVATE_PRIORITY + ":" + priorityArray[1]);
                    }
                } else {
                    fixedSortList.add(key);
                }
            }
            // Add new fixed sort list
            options.put(QueryOptions.SORT, fixedSortList);
        }

        return options;
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(jobCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(jobCollection, bsonQuery, field, "name", fixOptions(options));
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(jobCollection, bsonQuery, fields, "name", fixOptions(options));
    }

    @Override
    public OpenCGAResult groupBy(long studyUid, Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, studyUid);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Enums.Resource.JOB.name());
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(jobCollection, bsonQuery, field, QueryParams.NAME.key(), fixOptions(options));
    }

    @Override
    public OpenCGAResult groupBy(long studyUid, Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(null, studyUid);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Enums.Resource.JOB.name());
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(jobCollection, bsonQuery, fields, QueryParams.NAME.key(), fixOptions(options));
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<Job> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    void removeFileReferences(ClientSession clientSession, long studyUid, long fileUid, Document file) {
        UpdateDocument document = new UpdateDocument();

        String prefix = QueryParams.ATTRIBUTES.key() + "." + Constants.PRIVATE_OPENCGA_ATTRIBUTES + ".";

        // INPUT
        Document query = new Document()
                .append(PRIVATE_STUDY_UID, studyUid)
                .append(QueryParams.INPUT_UID.key(), fileUid);
        document.getPullAll().put(QueryParams.INPUT.key(),
                Collections.singletonList(new Document(FileDBAdaptor.QueryParams.UID.key(), fileUid)));
        document.getPush().put(prefix + Constants.JOB_DELETED_INPUT_FILES, file);
        Document updateDocument = document.toFinalUpdateDocument();

        logger.debug("Removing file from job '{}' field. Query: {}, Update: {}", QueryParams.INPUT.key(),
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = jobCollection.update(clientSession, query, updateDocument, QueryOptions.empty());
        logger.debug("File '{}' removed from {} jobs", fileUid, result.getNumUpdated());

        // OUTPUT
        query = new Document()
                .append(PRIVATE_STUDY_UID, studyUid)
                .append(QueryParams.OUTPUT_UID.key(), fileUid);
        document = new UpdateDocument();
        document.getPullAll().put(QueryParams.OUTPUT.key(),
                Collections.singletonList(new Document(FileDBAdaptor.QueryParams.UID.key(), fileUid)));
        document.getPush().put(prefix + Constants.JOB_DELETED_OUTPUT_FILES, file);
        updateDocument = document.toFinalUpdateDocument();

        logger.debug("Removing file from job '{}' field. Query: {}, Update: {}", QueryParams.OUTPUT.key(),
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        result = jobCollection.update(clientSession, query, updateDocument, QueryOptions.empty());
        logger.debug("File '{}' removed from {} jobs", fileUid, result.getNumUpdated());

        // OUT DIR
        query = new Document()
                .append(PRIVATE_STUDY_UID, studyUid)
                .append(QueryParams.OUT_DIR_UID.key(), fileUid);
        document = new UpdateDocument();
        document.getSet().put(QueryParams.OUT_DIR.key(), new Document(FileDBAdaptor.QueryParams.UID.key(), -1));
        document.getSet().put(prefix + Constants.JOB_DELETED_OUTPUT_DIRECTORY, file);
        updateDocument = document.toFinalUpdateDocument();

        logger.debug("Removing file from job '{}' field. Query: {}, Update: {}", QueryParams.OUT_DIR.key(),
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        result = jobCollection.update(clientSession, query, updateDocument, QueryOptions.empty());
        logger.debug("File '{}' removed from {} jobs", fileUid, result.getNumUpdated());
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    protected Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = (QueryParams.getParam(entry.getKey()) != null)
                    ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case RESOURCE_MANAGER_ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case INPUT:
                    case INPUT_UID:
                        addAutoOrQuery(QueryParams.INPUT_UID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                    case OUTPUT:
                    case OUTPUT_UID:
                        addAutoOrQuery(QueryParams.OUTPUT_UID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                Status.getPositiveStatus(Job.JobStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case NAME:
                    case UUID:
                    case USER_ID:
                    case TOOL_NAME:
                    case TYPE:
                    case DESCRIPTION:
                    case PRIORITY:
                    case START_TIME:
                    case END_TIME:
                    case OUTPUT_ERROR:
                    case EXECUTION:
                    case COMMAND_LINE:
                    case VISITED:
                    case RELEASE:
                    case STATUS:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case SIZE:
                    case OUT_DIR_UID:
                    case TMP_OUT_DIR_URI:
                    case TAGS:
                    case ERROR:
                    case ERROR_DESCRIPTION:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (authorisation != null && authorisation.size() > 0) {
            andBsonList.add(authorisation);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
