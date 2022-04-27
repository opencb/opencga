/*
 * Copyright 2015-2020 OpenCB
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
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.JobCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobAclEntry;
import org.opencb.opencga.core.models.job.JobInternalWebhook;
import org.opencb.opencga.core.models.job.ToolInfo;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
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
    private static final String PRIVATE_STUDY_UIDS = "_studyUids";

    public JobMongoDBAdaptor(MongoDBCollection jobCollection, MongoDBCollection deletedJobCollection, Configuration configuration,
                             MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(JobMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.jobCollection = jobCollection;
        this.deletedJobCollection = deletedJobCollection;
        this.jobConverter = new JobConverter();
    }

    /**
     * @return MongoDB connection to the job collection.
     */
    public MongoDBCollection getJobCollection() {
        return jobCollection;
    }

    public boolean exists(ClientSession clientSession, long jobUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(clientSession, new Query(QueryParams.UID.key(), jobUid)).getNumMatches() > 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> job, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(job, "job");
        return new OpenCGAResult(jobCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, Job job, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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

        long jobUid = getNewUid();
        job.setUid(jobUid);
        job.setStudyUid(studyId);
        if (StringUtils.isEmpty(job.getUuid())) {
            job.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.JOB));
        }
        if (StringUtils.isEmpty(job.getCreationDate())) {
            job.setCreationDate(TimeUtils.getTime());
        }
        if (job.getPriority() == null) {
            job.setPriority(Enums.Priority.LOW);
        }

        Document jobObject = jobConverter.convertToStorageType(job);
        jobObject.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(job.getCreationDate()));
        jobObject.put(PRIVATE_MODIFICATION_DATE, TimeUtils.toDate(job.getModificationDate()));
        jobObject.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        jobObject.put(PRIVATE_PRIORITY, job.getPriority().getValue());
        jobObject.put(PRIVATE_STUDY_UIDS, Collections.singletonList(studyId));

        logger.debug("Inserting job '{}' ({})...", job.getId(), job.getUid());
        jobCollection.insert(clientSession, jobObject, null);
        logger.debug("Job '{}' successfully inserted", job.getId());
        return jobUid;
    }

    @Override
    public OpenCGAResult<Job> getAllInStudy(long studyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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
    public long getStudyId(long jobId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), jobId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, PRIVATE_STUDY_UID);
        OpenCGAResult<Document> queryResult = nativeGet(query, queryOptions);

        if (queryResult.getNumResults() != 0) {
            Object id = queryResult.first().get(PRIVATE_STUDY_UID);
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
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonDocument = parseQuery(query, QueryOptions.empty());
        return new OpenCGAResult<>(jobCollection.count(clientSession, bsonDocument));
    }


    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, QueryOptions.empty(), user);
        logger.debug("Job count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(jobCollection.count(bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult update(long jobUid, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Job> dataResult = get(jobUid, options);

        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update job. Job uid '" + jobUid + "' not found.");
        }

        try {
            return runTransaction(session -> privateUpdate(session, dataResult.first(), parameters, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update job {}: {}", dataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update job " + dataResult.first().getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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
                result.append(runTransaction(session -> privateUpdate(session, job, parameters, queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update job {}: {}", job.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, job.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Job job, ObjectMap parameters, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        Document jobParameters = parseAndValidateUpdateParams(parameters, options).toFinalUpdateDocument();
        if (jobParameters.isEmpty()) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to update. Empty 'parameters' object");
        }

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), job.getStudyUid())
                .append(QueryParams.UID.key(), job.getUid());
        Bson finalQuery = parseQuery(tmpQuery, options);

        logger.debug("Job update: query : {}, update: {}",
                finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                jobParameters.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = jobCollection.update(clientSession, finalQuery, jobParameters, null);

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
    public OpenCGAResult delete(Job job) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<Document> iterator = nativeIterator(query, QueryOptions.empty());

        OpenCGAResult<Job> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Document job = iterator.next();
            String jobId = job.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, job)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
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
        nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new InternalStatus(InternalStatus.DELETED), "status"), jobDocument);

        // Upsert the document into the DELETED collection
        Bson query = new Document()
                .append(QueryParams.ID.key(), jobId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedJobCollection.update(clientSession, query, new Document("$set", replaceDotsInKeys(jobDocument)),
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

    private UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, QueryOptions options) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {QueryParams.USER_ID.key(), QueryParams.DESCRIPTION.key(), QueryParams.COMMAND_LINE.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        String[] acceptedBooleanParams = {QueryParams.VISITED.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        String[] acceptedStringListParams = {QueryParams.TAGS.key()};
        filterStringListParams(parameters, document.getSet(), acceptedStringListParams);

        if (parameters.containsKey(QueryParams.TOOL.key())) {
            if (parameters.get(QueryParams.TOOL.key()) instanceof ToolInfo) {
                document.getSet().put(QueryParams.TOOL.key(), getMongoDBDocument(parameters.get(QueryParams.TOOL.key()),
                        ToolInfo.class.getName()));
            } else {
                document.getSet().put(QueryParams.TOOL.key(), parameters.get(QueryParams.TOOL.key()));
            }
        }

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_ID.key(), parameters.get(QueryParams.INTERNAL_STATUS_ID.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_DESCRIPTION.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_DESCRIPTION.key(),
                    parameters.get(QueryParams.INTERNAL_STATUS_DESCRIPTION.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.INTERNAL_WEBHOOK.key())) {
            Object value = parameters.get(QueryParams.INTERNAL_WEBHOOK.key());
            if (value instanceof JobInternalWebhook) {
                document.getSet().put(QueryParams.INTERNAL_WEBHOOK.key(), getMongoDBDocument(value, "JobInternalWebhook"));
            } else {
                document.getSet().put(QueryParams.INTERNAL_WEBHOOK.key(), value);
            }
        }

        if (parameters.containsKey(QueryParams.INTERNAL_EVENTS.key())) {
            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.INTERNAL_EVENTS.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] acceptedObjectParams = new String[]{QueryParams.INTERNAL_EVENTS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPullAll(), acceptedObjectParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), acceptedObjectParams);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (parameters.containsKey(QueryParams.INPUT.key())) {
            List<Object> fileList = parameters.getList(QueryParams.INPUT.key());
            document.getSet().put(QueryParams.INPUT.key(), jobConverter.convertFilesToDocument(fileList));
        }
        if (parameters.containsKey(QueryParams.OUTPUT.key())) {
            List<Object> fileList = parameters.getList(QueryParams.OUTPUT.key());
            document.getSet().put(QueryParams.OUTPUT.key(), jobConverter.convertFilesToDocument(fileList));
        }
        if (parameters.containsKey(QueryParams.OUT_DIR.key())) {
            document.getSet().put(QueryParams.OUT_DIR.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.OUT_DIR.key())));
        }
        if (parameters.containsKey(QueryParams.STDOUT.key())) {
            document.getSet().put(QueryParams.STDOUT.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.STDOUT.key())));
        }
        if (parameters.containsKey(QueryParams.STDERR.key())) {
            document.getSet().put(QueryParams.STDERR.key(), jobConverter.convertFileToDocument(parameters.get(QueryParams.STDERR.key())));
        }

        if (parameters.containsKey(QueryParams.PRIORITY.key())) {
            document.getSet().put(QueryParams.PRIORITY.key(), parameters.getString(QueryParams.PRIORITY.key()));
            document.getSet().put(PRIVATE_PRIORITY,
                    Enums.Priority.getPriority(parameters.getString(QueryParams.PRIORITY.key())).getValue());
        }

        String[] acceptedObjectParams = {QueryParams.EXECUTION.key(), QueryParams.STUDY.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (document.getSet().containsKey(QueryParams.STUDY.key())) {
            List<String> studyFqns = new LinkedList<>();
            studyFqns.add(parameters.getString(QueryParams.STUDY_ID.key()));
            studyFqns.addAll(parameters.getAsStringList(QueryParams.STUDY_OTHERS.key()));
            Query query = new Query(StudyDBAdaptor.QueryParams.FQN.key(), studyFqns);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.UID.key());
            OpenCGAResult<Study> studyResults = dbAdaptorFactory.getCatalogStudyDBAdaptor().get(query, queryOptions);
            if (studyResults.getNumResults() < studyFqns.size()) {
                throw new CatalogDBException("Unable to find some studies from '" + studyFqns + "'");
            }

            // Add uids to others array
            document.getSet().put(PRIVATE_STUDY_UIDS,
                    studyResults.getResults().stream().map(Study::getUid).collect(Collectors.toList()));
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    public OpenCGAResult clean(int id) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), id);
        OpenCGAResult<Job> jobDataResult = get(query, null);
        if (jobDataResult.getResults().size() == 1) {
            DataResult delete = jobCollection.remove(parseQuery(query, QueryOptions.empty()), null);
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
    public OpenCGAResult<Job> get(long jobId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(jobId);
        Query query = new Query(QueryParams.UID.key(), jobId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(jobId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Job> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Job> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Job> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Job> dbIterator = iterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Job> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options);
        return new JobCatalogMongoDBIterator(mongoCursor, null, jobConverter, this, dbAdaptorFactory.getCatalogFileDBAdaptor(), options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    public DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new JobCatalogMongoDBIterator(mongoCursor, null, null, this, dbAdaptorFactory.getCatalogFileDBAdaptor(), options);
    }

    @Override
    public DBIterator<Job> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);
        return new JobCatalogMongoDBIterator(mongoCursor, null, jobConverter, this, dbAdaptorFactory.getCatalogFileDBAdaptor(), options,
                studyUid, user);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    public DBIterator nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        return new JobCatalogMongoDBIterator(mongoCursor, null, null, this, dbAdaptorFactory.getCatalogFileDBAdaptor(), options, studyUid,
                user);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = fixOptions(qOptions);

        Bson bson = parseQuery(query, options, user);

        logger.debug("Job get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return jobCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedJobCollection.iterator(clientSession, bson, null, null, qOptions);
        }
    }

    private QueryOptions fixOptions(QueryOptions queryOptions) {
        QueryOptions options = new QueryOptions(queryOptions);

        filterOptions(options, FILTER_ROUTE_JOBS);
        fixAclProjection(options);
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
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query, QueryOptions.empty());
        return rank(jobCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query, options);
        return groupBy(jobCollection, bsonQuery, field, "name", fixOptions(options));
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query, options);
        return groupBy(jobCollection, bsonQuery, fields, "name", fixOptions(options));
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, options, user);
        return groupBy(jobCollection, bsonQuery, fields, QueryParams.ID.key(), fixOptions(options));
    }

    @Override
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, null, userId);

        return new OpenCGAResult(jobCollection.distinct(field, bson, clazz));
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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

    private Bson parseQuery(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, options, null);
    }

    private Bson parseQuery(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, options, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, options, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();

        boolean mainStudy = true;
        if (options != null) {
            mainStudy = !options.getBoolean(ParamConstants.OTHER_STUDIES_FLAG, false);
        }

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.JOB, user, configuration));
            } else {
                // Get the document query needed to check the permissions as well
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, JobAclEntry.JobPermissions.VIEW.name(),
                        Enums.Resource.JOB, configuration));
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

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
                        if (mainStudy) {
                            addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        } else {
                            addAutoOrQuery(PRIVATE_STUDY_UIDS, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        }
                        break;
                    case TOOL:
                    case TOOL_ID:
                        addAutoOrQuery(QueryParams.TOOL_ID.key(), queryParam.key(), queryCopy, QueryParams.TOOL_ID.type(), andBsonList);
                        break;
                    case INPUT_UID:
                        addAutoOrQuery(QueryParams.INPUT_UID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case OUTPUT_UID:
                        addAutoOrQuery(QueryParams.OUTPUT_UID.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), queryCopy, QueryParams.STATUS_ID.type(),
                                andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(Enums.ExecutionStatus.STATUS_LIST, queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), queryCopy,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case USER_ID:
                    case TOOL_TYPE:
                    case PRIORITY: // TODO: This filter is not indexed. We should change it and query _priority instead.
//                    case START_TIME:
//                    case END_TIME:
//                    case OUTPUT_ERROR:
//                    case EXECUTION_START:
//                    case EXECUTION_END:
//                    case COMMAND_LINE:
                    case VISITED:
                    case RELEASE:
                    case OUT_DIR_UID:
                    case TAGS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
