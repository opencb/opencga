package org.opencb.opencga.catalog.db.mongodb;

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
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.catalog.models.User;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoJobDBAdaptor extends CatalogMongoDBAdaptor implements CatalogJobDBAdaptor {

    private final MongoDBCollection jobCollection;
    private JobConverter jobConverter;

    public CatalogMongoJobDBAdaptor(MongoDBCollection jobCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoJobDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.jobCollection = jobCollection;
        this.jobConverter = new JobConverter();
    }


    @Override
    public QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        this.dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        int jobId = getNewId();
        job.setId(jobId);

        Document jobObject = getMongoDBDocument(job, "job");
        jobObject.put(PRIVATE_ID, jobId);
        jobObject.put(PRIVATE_STUDY_ID, studyId);
        QueryResult insertResult = jobCollection.insert(jobObject, null); //TODO: Check results.get(0).getN() != 0

        return endQuery("Create Job", startTime, getJob(jobId, filterOptions(options, FILTER_ROUTE_JOBS)));
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
    public QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
//        QueryResult<DBObject> queryResult = jobCollection.find(new BasicDBObject(PRIVATE_ID, jobId),
//                          filterOptions(options, FILTER_ROUTE_JOBS));
        QueryResult<Document> queryResult = jobCollection.find(Filters.eq(PRIVATE_ID, jobId), filterOptions(options, FILTER_ROUTE_JOBS));
        Job job = parseJob(queryResult);
        if (job != null) {
            return endQuery("Get job", startTime, Arrays.asList(job));
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> getAllJobsInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        // Check the studyId first and throw an Exception is not found
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        // Retrieve and return Jobs
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return get(query, options);
    }

    @Override
    public String getJobStatus(int jobId, String sessionId) throws CatalogDBException {   // TODO remove?
        throw new UnsupportedOperationException("Not implemented method");
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject(PRIVATE_ID, jobId);
        Bson query = Filters.eq(PRIVATE_ID, jobId);
//        Job job = parseJob(jobCollection.<DBObject>find(query, new BasicDBObject("visits", true), null));

        Job job = getJob(jobId, new QueryOptions(MongoDBCollection.INCLUDE, "visits")).first();
        //Job job = parseJob(jobCollection.<DBObject>find(query, Projections.include("visits"), null));
        int visits;
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

    @Deprecated
    @Override
    public QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use update instead.");
        /*
        long startTime = startQuery();
        Map<String, Object> jobParameters = new HashMap<>();

        String[] acceptedParams = {"name", "userId", "toolName", "date", "description", "outputError", "commandLine", "status", "outdir",
                "error", "errorDescription", };
        filterStringParams(parameters, jobParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("status"), Job.Status.class);
        filterEnumParams(parameters, jobParameters, acceptedEnums);

        String[] acceptedIntParams = {"visits"};
        filterIntParams(parameters, jobParameters, acceptedIntParams);

        String[] acceptedLongParams = {"startTime", "endTime", "diskUsage"};
        filterLongParams(parameters, jobParameters, acceptedLongParams);

        String[] acceptedIntegerListParams = {"output"};
        filterIntegerListParams(parameters, jobParameters, acceptedIntegerListParams);
        if (parameters.containsKey("output")) {
            for (Integer fileId : parameters.getAsIntegerList("output")) {
//                checkFileExists(fileId, "Output File");
                dbAdaptorFactory.getCatalogFileDBAdaptor().checkFileId(fileId);
            }
        }

        String[] acceptedMapParams = {"attributes", "resourceManagerAttributes"};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        if (!jobParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject(PRIVATE_ID, jobId);
            BasicDBObject updates = new BasicDBObject("$set", jobParameters);
//            QueryResult<WriteResult> update = jobCollection.update(query, updates, null);
            QueryResult<UpdateResult> update = jobCollection.update(query, updates, null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Job", jobId);
            }
        }
        return endQuery("Modify job", startTime, getJob(jobId, null));
        */
    }

    @Override
    public int getStudyIdByJobId(int jobId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), jobId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = nativeGet(query, queryOptions);

        if (queryResult.getNumResults() != 0) {
            Object id = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Deprecated
    @Override
    public QueryResult<Job> getAllJobs(Query query, QueryOptions options) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use get instead.");
        /*
        long startTime = startQuery();

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
        */
    }

    @Override
    public QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException {
        long startTime = startQuery();

        if (!dbAdaptorFactory.getCatalogUserDBAdaptor().userExists(userId)) {
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
            throw new CatalogDBException("Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
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
            throw new CatalogDBException("Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
        }

        return endQuery("Create tool", startTime, getTool(tool.getId()).getResult());
    }

    public QueryResult<Tool> getTool(int id) throws CatalogDBException {
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
    public int getToolId(String userId, String toolAlias) throws CatalogDBException {
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
                Aggregates.match(parseQuery(query))
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
    public boolean experimentExists(int experimentId) {
        return false;
    }


    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return jobCollection.count(bsonDocument);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
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
        Bson bson = parseQuery(query);
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
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        //qOptions.append(MongoDBCollection.EXCLUDE, Arrays.asList(PRIVATE_ID, PRIVATE_STUDY_ID));
        qOptions = filterOptions(qOptions, FILTER_ROUTE_JOBS);
        return jobCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> jobParameters = new HashMap<>();

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.USER_ID.key(), QueryParams.TOOL_NAME.key(), QueryParams.DATE.key()
                , QueryParams.DESCRIPTION.key(), QueryParams.OUTPUT_ERROR.key(), QueryParams.COMMAND_LINE.key(), QueryParams.STATUS.key()
                , QueryParams.OUT_DIR_ID.key(), QueryParams.ERROR.key(), QueryParams.ERROR_DESCRIPTION.key(), };
        filterStringParams(parameters, jobParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap((QueryParams.STATUS.key()), Job.Status.class);
        filterEnumParams(parameters, jobParameters, acceptedEnums);

        String[] acceptedIntParams = {QueryParams.VISITS.key(), };
        filterIntParams(parameters, jobParameters, acceptedIntParams);

        String[] acceptedLongParams = {QueryParams.START_TIME.key(), QueryParams.END_TIME.key(), QueryParams.DISK_USAGE.key()};
        filterLongParams(parameters, jobParameters, acceptedLongParams);

        String[] acceptedIntegerListParams = {QueryParams.OUTPUT.key()};
        filterIntegerListParams(parameters, jobParameters, acceptedIntegerListParams);
        if (parameters.containsKey(QueryParams.OUTPUT.key())) {
            for (Integer fileId : parameters.getAsIntegerList(QueryParams.OUTPUT.key())) {
//                checkFileExists(fileId, "Output File");
                dbAdaptorFactory.getCatalogFileDBAdaptor().checkFileId(fileId);
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key()};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        if (!jobParameters.isEmpty()) {
            QueryResult<UpdateResult> update = jobCollection.update(parseQuery(query), new Document("$set", jobParameters), null);
            return endQuery("Update job", startTime, Arrays.asList(update.getNumTotalResults()));
        }
        return endQuery("Update job", startTime, new QueryResult<Long>());
    }

    @Override
    public QueryResult<Job> update(int id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        checkJobId(id);
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Long> update = update(query, parameters);
        if (update.getResult().isEmpty() || update.first() != 1) {
            throw new CatalogDBException("Could not update job " + id);
        }
        return endQuery("Update user", startTime, get(query, null));
    }

    @Override
    public QueryResult<Job> delete(int id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Job> jobQueryResult = get(query, null);
        if (jobQueryResult.getResult().size() == 1) {
            QueryResult<Long> delete = delete(query);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Job id '{}' has not been deleted", id);
            }
        } else {
            throw CatalogDBException.idNotFound("Job id '{}' does not exist (or there are too many)", id);
        }
        return jobQueryResult;
    }

    @Override
    public QueryResult<Long> delete(Query query) throws CatalogDBException {
        long timeStart = startQuery();
        QueryResult<DeleteResult> remove = jobCollection.remove(parseQuery(query), null);
        return endQuery("Delete job", timeStart, Collections.singletonList(remove.getNumTotalResults()));
    }

    @Override
    public CatalogDBIterator<Job> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = jobCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, jobConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = jobCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(jobCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(jobCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(jobCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Job> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = (QueryParams.getParam(entry.getKey()) != null)
                    ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
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
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
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
