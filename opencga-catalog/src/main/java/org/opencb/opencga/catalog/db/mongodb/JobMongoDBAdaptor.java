/*
 * Copyright 2015-2016 OpenCB
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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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


    @Override
    public QueryResult<Job> insert(Job job, long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        this.dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

        long jobId = getNewId();
        job.setId(jobId);

        Document jobObject = jobConverter.convertToStorageType(job);
        jobObject.put(PRIVATE_ID, jobId);
        jobObject.put(PRIVATE_STUDY_ID, studyId);
        jobCollection.insert(jobObject, null); //TODO: Check results.get(0).getN() != 0

        return endQuery("Create Job", startTime, get(jobId, filterOptions(options, FILTER_ROUTE_JOBS)));
    }

    @Override
    public QueryResult<Long> extractFilesFromJobs(Query query, List<Long> fileIds) throws CatalogDBException {
        if (fileIds == null || fileIds.size() == 0) {
            throw new CatalogDBException("The array of fileIds is empty");
        }

        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);

        Query queryAux = new Query(query);
        queryAux.append(QueryParams.OUT_DIR_ID.key(), fileIds);
        ObjectMap params = new ObjectMap(QueryParams.OUT_DIR_ID.key(), -1);
        Document update = new Document("$set", params);
        QueryResult<UpdateResult> update1 = jobCollection.update(parseQuery(queryAux, true), update, multi);
        logger.debug("{} out of {} documents changed to have outDirId = -1", update1.first().getMatchedCount(),
                update1.first().getModifiedCount());

        queryAux = new Query(query);
        queryAux.append(QueryParams.INPUT.key(), fileIds);
        update = new Document("$pullAll", new Document(QueryParams.INPUT.key(), fileIds));
        QueryResult<UpdateResult> update2 = jobCollection.update(parseQuery(queryAux, true), update, multi);
        logger.debug("{} out of {} documents changed to pull input file ids", update2.first().getMatchedCount(),
                update2.first().getModifiedCount());

        queryAux = new Query(query);
        queryAux.append(QueryParams.OUTPUT.key(), fileIds);
        update = new Document("$pullAll", new Document(QueryParams.OUTPUT.key(), fileIds));
        QueryResult<UpdateResult> update3 = jobCollection.update(parseQuery(queryAux, true), update, multi);
        logger.debug("{} out of {} documents changed to pull input file ids", update3.first().getMatchedCount(),
                update3.first().getModifiedCount());

        QueryResult retResult = new QueryResult("Extract fileIds from jobs");
        retResult.setNumResults(1);
        retResult.setNumTotalResults(1);
        retResult.setResult(Arrays.asList(update1.first().getModifiedCount() + update2.first().getModifiedCount()
                + update3.first().getModifiedCount()));
        return retResult;
    }

    /**
     * At the moment it does not clean external references to itself.
     */
//    @Override
//    public QueryResult<Job> deleteJob(int jobId) throws CatalogDBException {
//        long startTime = startQuery();
//        Job job = getJob(jobId, null).first();
//        WriteResult id = jobCollection.remove(new BasicDBObject(PRIVATE_ID, jobId), null).getResult().get(0);
//        List<Integer> deletes = new LinkedList<>();
//        if (id.getN() == 0) {
//            throw CatalogDBException.idNotFound("Job", jobId);
//        } else {
//            deletes.add(id.getN());
//            return endQuery("delete job", startTime, Collections.singletonList(job));
//        }
//    }
    @Override
    public QueryResult<Job> get(long jobId, QueryOptions options) throws CatalogDBException {
        checkId(jobId);
        return get(new Query(QueryParams.ID.key(), jobId).append(QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED), options);
//        long startTime = startQuery();
//        QueryResult<Document> queryResult = jobCollection.find(Filters.eq(PRIVATE_ID, jobId), filterOptions(options, FILTER_ROUTE_JOBS));
//        Job job = parseJob(queryResult);
//        if (job != null) {
//            return endQuery("Get job", startTime, Arrays.asList(job));
//        } else {
//            throw CatalogDBException.idNotFound("Job", jobId);
//        }
    }

    @Override
    public QueryResult<Job> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        // Check the studyId first and throw an Exception is not found
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

        // Retrieve and return Jobs
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return get(query, options);
    }

    @Override
    public String getStatus(long jobId, String sessionId) throws CatalogDBException {   // TODO remove?
        throw new UnsupportedOperationException("Not implemented method");
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(long jobId) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject(PRIVATE_ID, jobId);
        Bson query = Filters.eq(PRIVATE_ID, jobId);
//        Job job = parseJob(jobCollection.<DBObject>find(query, new BasicDBObject("visits", true), null));

        Job job = get(jobId, new QueryOptions(MongoDBCollection.INCLUDE, "visits")).first();
        //Job job = parseJob(jobCollection.<DBObject>find(query, Projections.include("visits"), null));
        long visits;
        if (job != null) {
            visits = job.getVisits() + 1;
//            BasicDBObject set = new BasicDBObject("$set", new BasicDBObject("visits", visits));
            Bson set = Updates.set("visits", visits);
            jobCollection.update(query, set, null);
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
        return endQuery("Inc visits", startTime, Collections.singletonList(new ObjectMap("visits", visits)));
    }

    @Override
    public long getStudyId(long jobId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), jobId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = nativeGet(query, queryOptions);

        if (queryResult.getNumResults() != 0) {
            Object id = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return id instanceof Number ? ((Number) id).longValue() : Long.parseLong(id.toString());
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    @Deprecated
    public QueryResult<Long> extractFiles(List<Long> fileIds) throws CatalogDBException {
        long startTime = startQuery();
        long numResults;

        // Check for input
        Query query = new Query(QueryParams.INPUT.key(), fileIds);
        Bson bsonQuery = parseQuery(query, false);
        Bson update = new Document("$pull", new Document(QueryParams.INPUT.key(), new Document("$in", fileIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        numResults = jobCollection.update(bsonQuery, update, multi).first().getModifiedCount();

        // Check for output
        query = new Query(QueryParams.OUTPUT.key(), fileIds);
        bsonQuery = parseQuery(query, false);
        update = new Document("$pull", new Document(QueryParams.OUTPUT.key(), new Document("$in", fileIds)));
        numResults += jobCollection.update(bsonQuery, update, multi).first().getModifiedCount();

        return endQuery("Extract files from jobs", startTime, Collections.singletonList(numResults));
    }

    @Override
    public QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException {
        long startTime = startQuery();

        if (!dbAdaptorFactory.getCatalogUserDBAdaptor().exists(userId)) {
            throw new CatalogDBException("User {id:" + userId + "} does not exist");
        }

//         Check if tools.alias already exists.
//        DBObject countQuery = BasicDBObjectBuilder
//                .start(PRIVATE_ID, userId)
//                .append("tools.alias", tool.getAlias())
//                .get();
//        QueryResult<Long> count = userCollection.count(countQuery);

        Query query1 = new Query(QueryParams.ID.key(), userId).append("tools.alias", tool.getAlias());
        QueryResult<Long> count = dbAdaptorFactory.getCatalogUserDBAdaptor().count(query1);
        if (count.getResult().get(0) != 0) {
            throw new CatalogDBException("Tool {alias:\"" + tool.getAlias() + "\"} already exists for this user");
        }

        // Create and add the new tool
        tool.setId(getNewId());

        Document toolObject = getMongoDBDocument(tool, "tool");
        Document query = new Document(PRIVATE_ID, userId);
        query.put("tools.alias", new Document("$ne", tool.getAlias()));

//        DBObject update = new BasicDBObject("$push", new BasicDBObject("tools", toolObject));
        Bson push = Updates.push("tools", toolObject);
        //Update object
//        QueryResult<WriteResult> queryResult = userCollection.update(query, update, null);
        QueryResult<UpdateResult> queryResult = dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection().update(query, push, null);

        if (queryResult.getResult().get(0).getModifiedCount() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Tool {alias:\"" + tool.getAlias() + "\"} already exists for this user");
        }

        return endQuery("Create tool", startTime, getTool(tool.getId()).getResult());
    }

    @Override
    public QueryResult<Tool> getTool(long id) throws CatalogDBException {
        long startTime = startQuery();

//        DBObject query = new BasicDBObject("tools.id", id);
//        DBObject projection = new BasicDBObject("tools",
//                new BasicDBObject("$elemMatch",
//                        new BasicDBObject("id", id)
//                )
//        );
//        QueryResult<DBObject> queryResult = userCollection.find(query, projection, new QueryOptions("include", Collections
// .singletonList("tools")));

        Bson query = Filters.eq("tools.id", id);
        Bson projection = Projections.fields(Projections.elemMatch("tools", Filters.eq("id", id)), Projections.include("tools"));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection()
                .find(query, projection, new QueryOptions());

        if (queryResult.getNumResults() != 1) {
            throw new CatalogDBException("Tool {id:" + id + "} no exists");
        }

        User user = parseUser(queryResult);
        return endQuery("Get tool", startTime, user.getTools());
    }

    @Override
    public long getToolId(String userId, String toolAlias) throws CatalogDBException {
//        DBObject query = BasicDBObjectBuilder
//                .start(PRIVATE_ID, userId)
//                .append("tools.alias", toolAlias).get();
//        DBObject projection = new BasicDBObject("tools",
//                new BasicDBObject("$elemMatch",
//                        new BasicDBObject("alias", toolAlias)
//                )
//        );
//        QueryResult<DBObject> queryResult = userCollection.find(query, projection, null);

        Bson query = Filters.and(Filters.eq(PRIVATE_ID, userId), Filters.eq("tools.alias", toolAlias));
        Bson projection = Projections.elemMatch("tools", Filters.eq("alias", toolAlias));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection().find(query,
                projection, new QueryOptions());

        if (queryResult.getNumResults() != 1) {
            throw new CatalogDBException("Tool {alias:" + toolAlias + "} no exists");
        }
        User user = parseUser(queryResult);
        return user.getTools().get(0).getId();
    }

    @Override
    public QueryResult<Tool> getAllTools(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        List<Bson> aggregations = Arrays.asList(
                Aggregates.project(Projections.include("tools")),
                Aggregates.unwind("$tools"),
                Aggregates.match(parseQuery(query, false))
        );
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection()
                .aggregate(aggregations, queryOptions);

/*        DBObject query = new BasicDBObject();
        addQueryStringListFilter("userId", queryOptions, PRIVATE_ID, query);
        addQueryIntegerListFilter("id", queryOptions, "tools.id", query);
        addQueryIntegerListFilter("alias", queryOptions, "tools.alias", query);

        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogUserDBAdaptor().getUserCollection().aggregate(
                Arrays.asList(
                        new BasicDBObject("$project", new BasicDBObject("tools", 1)),
                        new BasicDBObject("$unwind", "$tools"),
                        new BasicDBObject("$match", query)
                ), queryOptions);

*/
        List<User> users = parseObjects(queryResult, User.class);
        List<Tool> tools = users.stream().map(user -> user.getTools().get(0)).collect(Collectors.toList());
        return endQuery("Get tools", startTime, tools);
    }

    @Override
    public boolean experimentExists(long experimentId) {
        return false;
    }


    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query, false);
        return jobCollection.count(bsonDocument);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query, false);
        return jobCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Job> get(Query query, QueryOptions options) throws CatalogDBException {

        // FIXME: Take into account the following commented code:
        /*
        * long startTime = startQuery();

        Document mongoQuery = new Document();

        if (query.containsKey("ready")) {
            if (query.getBoolean("ready")) {
                mongoQuery.put("status", Job.Status.READY.name());
            } else {
                mongoQuery.put("status", new BasicDBObject("$ne", Job.Status.READY.name()));
            }
            query.remove("ready");
        }

//        if (query.containsKey("studyId")) {
//            addQueryIntegerListFilter("studyId", query, PRIVATE_STUDY_ID, mongoQuery);
//        }
//
//        if (query.containsKey("status")) {
//            addQueryStringListFilter("status", query, mongoQuery);
//        }

//        System.out.println("query = " + query);
//        QueryResult<DBObject> queryResult = jobCollection.find(mongoQuery, null);
        QueryResult<Document> queryResult = jobCollection.find(mongoQuery, null);
        List<Job> jobs = parseJobs(queryResult);
        return endQuery("Search job", startTime, jobs);
        * */

        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_JOBS);
        QueryResult<Job> jobQueryResult = jobCollection.find(bson, jobConverter, qOptions);
        return endQuery("Get job", startTime, jobQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED);
        }
        Bson bson = parseQuery(query, false);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }

        qOptions = filterOptions(qOptions, FILTER_ROUTE_JOBS);
        return jobCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> jobParameters = getValidatedUpdateParams(parameters);

        if (!jobParameters.isEmpty()) {
            QueryResult<UpdateResult> update = jobCollection.update(parseQuery(query, false), new Document("$set", jobParameters), null);
            return endQuery("Update job", startTime, Arrays.asList(update.getNumTotalResults()));
        }
        return endQuery("Update job", startTime, new QueryResult<Long>());
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = jobCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("Job");
        }
    }

    @Override
    public QueryResult<Job> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = parseQuery(new Query(QueryParams.ID.key(), id), true);
        Map<String, Object> myParams = getValidatedUpdateParams(parameters);

        if (myParams.isEmpty()) {
            logger.debug("The map of parameters to update the job is empty. It originally contained {}", parameters.safeToString());
            throw new CatalogDBException("Nothing to update");
        }

        logger.debug("Update job. Query: {}, Update: {}",
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), myParams);

        QueryResult<UpdateResult> update = jobCollection.update(query, new Document("$set", myParams), new QueryOptions("multi", true));
        if (update.first().getMatchedCount() == 0) {
            throw new CatalogDBException("Job " + id + " not found.");
        }

        QueryResult<Job> queryResult = jobCollection.find(query, jobConverter, QueryOptions.empty());
        return endQuery("Update job", startTime, queryResult);
    }

    private Map<String, Object> getValidatedUpdateParams(ObjectMap parameters) throws CatalogDBException {
        Map<String, Object> jobParameters = new HashMap<>();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.USER_ID.key(), QueryParams.TOOL_NAME.key(),
                QueryParams.CREATION_DATE.key(), QueryParams.DESCRIPTION.key(), QueryParams.OUTPUT_ERROR.key(),
                QueryParams.COMMAND_LINE.key(), QueryParams.OUT_DIR_ID.key(), QueryParams.ERROR.key(), QueryParams.ERROR_DESCRIPTION.key(),
        };
        filterStringParams(parameters, jobParameters, acceptedParams);

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            jobParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            jobParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.STATUS.key()) && parameters.get(QueryParams.STATUS.key()) instanceof Job.JobStatus) {
            jobParameters.put(QueryParams.STATUS.key(), getMongoDBDocument(parameters.get(QueryParams.STATUS.key()), "Job.JobStatus"));
        }

        String[] acceptedIntParams = {QueryParams.VISITS.key(), };
        filterIntParams(parameters, jobParameters, acceptedIntParams);

        String[] acceptedLongParams = {QueryParams.START_TIME.key(), QueryParams.END_TIME.key(), QueryParams.SIZE.key()};
        filterLongParams(parameters, jobParameters, acceptedLongParams);

        String[] acceptedIntegerListParams = {QueryParams.OUTPUT.key()};
        filterIntegerListParams(parameters, jobParameters, acceptedIntegerListParams);
        if (parameters.containsKey(QueryParams.OUTPUT.key())) {
            for (Integer fileId : parameters.getAsIntegerList(QueryParams.OUTPUT.key())) {
                dbAdaptorFactory.getCatalogFileDBAdaptor().checkId(fileId);
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key()};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        return jobParameters;
    }


    public QueryResult<Job> clean(int id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Job> jobQueryResult = get(query, null);
        if (jobQueryResult.getResult().size() == 1) {
            QueryResult<DeleteResult> delete = jobCollection.remove(parseQuery(query, false), null);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Job id '{}' has not been deleted", id);
            }
        } else {
            throw CatalogDBException.idNotFound("Job id '{}' does not exist (or there are too many)", id);
        }
        return jobQueryResult;
    }

    @Override
    public QueryResult<Job> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore jobs", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Job> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The job {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore job", startTime, get(query, null));
    }

    @Override
    public DBIterator<Job> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        MongoCursor<Document> iterator = jobCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, jobConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        MongoCursor<Document> iterator = jobCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return rank(jobCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(jobCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(jobCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        DBIterator<Job> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = (QueryParams.getParam(entry.getKey()) != null)
                    ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                continue;
            }
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
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
                    case OUTPUT:
                        addQueryFilter(queryParam.key(), queryParam.key(), query, queryParam.type(),
                            MongoDBQueryUtils.ComparisonOperator.IN, MongoDBQueryUtils.LogicalOperator.OR, andBsonList);
                        break;
                    case NAME:
                    case USER_ID:
                    case TOOL_NAME:
                    case TYPE:
                    case CREATION_DATE:
                    case DESCRIPTION:
                    case START_TIME:
                    case END_TIME:
                    case OUTPUT_ERROR:
                    case EXECUTION:
                    case COMMAND_LINE:
                    case VISITS:
                    case STATUS:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case SIZE:
                    case OUT_DIR_ID:
                    case TMP_OUT_DIR_URI:
                    case TAGS:
                    case ACL:
                    case ACL_MEMBER:
                    case ACL_PERMISSIONS:
                    case ERROR:
                    case ERROR_DESCRIPTION:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

}
