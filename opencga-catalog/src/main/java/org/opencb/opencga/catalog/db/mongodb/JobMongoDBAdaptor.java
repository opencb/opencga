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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.MongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
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
    private JobConverter jobConverter;

    public JobMongoDBAdaptor(MongoDBCollection jobCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.jobCollection = jobCollection;
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
        return count(clientSession, new Query(QueryParams.UID.key(), jobUid)).first() > 0;
    }

    @Override
    public DataResult nativeInsert(Map<String, Object> job, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(job, "job");
        return jobCollection.insert(document, null);
    }

    @Override
    public DataResult insert(long studyId, Job job, QueryOptions options) throws CatalogDBException {
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
        filterList.add(Filters.eq(QueryParams.STATUS_NAME.key(), Status.READY));

        Bson bson = Filters.and(filterList);
        DataResult<Long> count = jobCollection.count(clientSession, bson);

        if (count.first() > 0) {
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

        Document jobObject = jobConverter.convertToStorageType(job);
        jobObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(job.getCreationDate()));
        jobObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting job '{}' ({})...", job.getId(), job.getUid());
        jobCollection.insert(clientSession, jobObject, null);
        logger.debug("Job '{}' successfully inserted", job.getId());
        return jobUid;
    }

    @Override
    public DataResult<Job> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
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
        DataResult<Document> queryResult = nativeGet(query, queryOptions);

        if (queryResult.getNumResults() != 0) {
            Object id = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return id instanceof Number ? ((Number) id).longValue() : Long.parseLong(id.toString());
        } else {
            throw CatalogDBException.uidNotFound("Job", jobId);
        }
    }

    @Override
    public DataResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(jobCollection, studyId, permissionRuleId);
    }

    @Override
    public DataResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    DataResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return jobCollection.count(clientSession, bsonDocument);
    }


    @Override
    public DataResult<Long> count(Query query, String user, StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_JOBS : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        DataResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getJobPermission().name(), Entity.JOB.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("Job count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return jobCollection.count(bson);
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);
        }
    }

    @Override
    public DataResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return jobCollection.distinct(field, bsonDocument);
    }

    @Override
    public DataResult stats(Query query) {
        return null;
    }

    @Override
    public DataResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single job
            if (count(query).first() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key() + "' can only be updated for one job");
            }
        }

        Document jobParameters = getValidatedUpdateParams(parameters);
        if (jobParameters.isEmpty()) {
            throw new CatalogDBException("Nothing to update. Empty 'parameters' object");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Job> iterator = iterator(query, options);

        int numMatches = 0;
        int numModified = 0;
        List<String> warnings = new ArrayList<>();

        while (iterator.hasNext()) {
            Job job = iterator.next();
            numMatches += 1;

            try {
                runTransaction(session -> {
                    long tmpStartTime = startQuery();
                    Query tmpQuery = new Query()
                            .append(QueryParams.STUDY_UID.key(), job.getStudyUid())
                            .append(QueryParams.UID.key(), job.getUid());
                    Bson finalQuery = parseQuery(tmpQuery);

                    logger.debug("Job update: query : {}, update: {}",
                            finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            jobParameters.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                    jobCollection.update(session, parseQuery(query), new Document("$set", jobParameters), null);

                    return endWrite(tmpStartTime, 1, 1, null);
                });
                logger.debug("Job {} successfully updated", job.getId());
                numModified += 1;
            } catch (CatalogDBException e) {
                String errorMsg = "Could not update job " + job.getId() + ": " + e.getMessage();
                logger.error(errorMsg);
                warnings.add(errorMsg);
            }
        }
        return endWrite(startTime, numMatches, numModified, warnings);
    }

    @Override
    public DataResult delete(Job job) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), job.getUid());
        DataResult delete = delete(query);
        if (delete.getNumMatches() == 0) {
            throw new CatalogDBException("Could not delete job. Uid " + job.getUid() + " not found.");
        } else if (delete.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not delete job.");
        }
        return delete;
    }

    @Override
    public DataResult delete(Query query) throws CatalogDBException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Job> iterator = iterator(query, options);

        long startTime = startQuery();
        int numMatches = 0;
        int numModified = 0;
        List<String> warnings = new ArrayList<>();

        while (iterator.hasNext()) {
            Job job = iterator.next();
            numMatches += 1;

            try {
                runTransaction(clientSession -> {
                    long tmpStartTime = startQuery();

                    logger.info("Deleting job {} ({})", job.getId(), job.getUid());

                    String deleteSuffix = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

                    Query tmpQuery = new Query()
                            .append(QueryParams.UID.key(), job.getUid())
                            .append(QueryParams.STUDY_UID.key(), job.getStudyUid());
                    // Mark the job as deleted
                    ObjectMap updateParams = new ObjectMap()
                            .append(QueryParams.STATUS_NAME.key(), Status.DELETED)
                            .append(QueryParams.STATUS_DATE.key(), TimeUtils.getTime())
                            .append(QueryParams.ID.key(), job.getId() + deleteSuffix);

                    Bson bsonQuery = parseQuery(tmpQuery);
                    Document updateDocument = getValidatedUpdateParams(updateParams);

                    logger.debug("Delete job {}: Query: {}, update: {}", job.getId(),
                            bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                    DataResult result = jobCollection.update(clientSession, bsonQuery, new Document("$set", updateDocument),
                            QueryOptions.empty());
                    if (result.getNumUpdated() == 1) {
                        logger.info("Job {}({}) deleted", job.getId(), job.getUid());
                    } else {
                        logger.error("Job '{}' successfully deleted", job.getId());
                    }

                    return endWrite(tmpStartTime, 1, 1, null);
                });
                logger.info("Job {} successfully deleted", job.getId());
                numModified += 1;
            } catch (CatalogDBException e) {
                String errorMsg = "Could not delete job " + job.getId() + ": " + e.getMessage();
                logger.error(errorMsg);
                warnings.add(errorMsg);
            }
        }

        return endWrite(startTime, numMatches, numModified, warnings);
    }

    @Override
    public DataResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        DataResult update = update(new Query(QueryParams.UID.key(), id), parameters, queryOptions);
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update job id '" + id + "'");
        }
        return update;
    }

    private Document getValidatedUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Document jobParameters = new Document();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.USER_ID.key(), QueryParams.TOOL_NAME.key(),
                QueryParams.CREATION_DATE.key(), QueryParams.DESCRIPTION.key(), QueryParams.OUTPUT_ERROR.key(),
                QueryParams.COMMAND_LINE.key(), QueryParams.ERROR.key(), QueryParams.ERROR_DESCRIPTION.key(),
        };
        filterStringParams(parameters, jobParameters, acceptedParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            jobParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            jobParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.STATUS.key()) && parameters.get(QueryParams.STATUS.key()) instanceof Job.JobStatus) {
            jobParameters.put(QueryParams.STATUS.key(), getMongoDBDocument(parameters.get(QueryParams.STATUS.key()), "Job.JobStatus"));
        }

        String[] acceptedBooleanParams = {QueryParams.VISITED.key(), };
        filterBooleanParams(parameters, jobParameters, acceptedBooleanParams);

        String[] acceptedLongParams = {QueryParams.START_TIME.key(), QueryParams.END_TIME.key(), QueryParams.SIZE.key(),
                QueryParams.OUT_DIR_UID.key(), };
        filterLongParams(parameters, jobParameters, acceptedLongParams);

        if (parameters.containsKey(QueryParams.INPUT.key())) {
            List<File> fileList = parameters.getAsList(QueryParams.INPUT.key(), File.class);
            jobParameters.put(QueryParams.INPUT.key(), jobConverter.convertFilesToDocument(fileList));
        }
        if (parameters.containsKey(QueryParams.OUTPUT.key())) {
            List<File> fileList = parameters.getAsList(QueryParams.OUTPUT.key(), File.class);
            jobParameters.put(QueryParams.OUTPUT.key(), jobConverter.convertFilesToDocument(fileList));
        }

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

    public DataResult clean(int id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
        DataResult<Job> jobDataResult = get(query, null);
        if (jobDataResult.getResults().size() == 1) {
            DataResult delete = jobCollection.remove(parseQuery(query), null);
            if (delete.getNumUpdated() == 0) {
                throw CatalogDBException.newInstance("Job id '{}' has not been deleted", id);
            }
            return delete;
        } else {
            throw CatalogDBException.uidNotFound("Job id '{}' does not exist (or there are too many)", id);
        }
    }


    @Override
    public DataResult remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public DataResult remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public DataResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.put(QueryParams.STATUS_NAME.key(), Status.DELETED);
        return setStatus(query, Status.READY);
    }

    @Override
    public DataResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The job {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        return setStatus(id, Status.READY);
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public DataResult<Job> get(long jobId, QueryOptions options) throws CatalogDBException {
        checkId(jobId);
        Query query = new Query(QueryParams.UID.key(), jobId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED)
                .append(QueryParams.STUDY_UID.key(), getStudyId(jobId));
        return get(query, options);
    }

    @Override
    public DataResult<Job> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Job> documentList = new ArrayList<>();
        DataResult<Job> queryResult;
        try (DBIterator<Job> dbIterator = iterator(query, options, user)) {
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
            DataResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_JOBS);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult<Job> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Job> documentList = new ArrayList<>();
        DataResult<Job> queryResult;
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
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        DataResult<Document> queryResult;
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
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        DataResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
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
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<Job> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new MongoDBIterator<>(mongoCursor, jobConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new MongoDBIterator<>(mongoCursor);
    }

    @Override
    public DBIterator<Job> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);
        return new MongoDBIterator<>(mongoCursor, jobConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, studyDocument, user);
        return new MongoDBIterator<>(mongoCursor);
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
                    StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Entity.JOB.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_JOBS);

        logger.debug("Job get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        return jobCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        DataResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }
        return queryResult.first();
    }

    @Override
    public DataResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return rank(jobCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(jobCollection, bsonQuery, field, "name", options);
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(jobCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Entity.JOB.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(jobCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                StudyAclEntry.StudyPermissions.VIEW_JOBS.name(), JobAclEntry.JobPermissions.VIEW.name(), Entity.JOB.name());
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(jobCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
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

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
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
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case RESOURCE_MANAGER_ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case INPUT:
                    case INPUT_UID:
                        addAutoOrQuery(QueryParams.INPUT_UID.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                    case OUTPUT:
                    case OUTPUT_UID:
                        addAutoOrQuery(QueryParams.OUTPUT_UID.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(),
                                Status.getPositiveStatus(Job.JobStatus.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ID:
                    case NAME:
                    case UUID:
                    case USER_ID:
                    case TOOL_NAME:
                    case TYPE:
                    case DESCRIPTION:
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
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
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
