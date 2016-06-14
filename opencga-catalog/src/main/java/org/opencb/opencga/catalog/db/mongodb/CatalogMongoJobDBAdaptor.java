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
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.JobConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.JobAcl;
import org.opencb.opencga.core.common.TimeUtils;
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
    public QueryResult<Job> createJob(long studyId, Job job, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        this.dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        long jobId = getNewId();
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
    public QueryResult<Job> getJob(long jobId, QueryOptions options) throws CatalogDBException {
        checkJobId(jobId);
        return get(new Query(QueryParams.ID.key(), jobId).append(QueryParams.STATUS_STATUS.key(), "!=" + Status.REMOVED), options);
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
    public QueryResult<Job> getAllJobsInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        // Check the studyId first and throw an Exception is not found
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

        // Retrieve and return Jobs
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return get(query, options);
    }

    @Override
    public String getJobStatus(long jobId, String sessionId) throws CatalogDBException {   // TODO remove?
        throw new UnsupportedOperationException("Not implemented method");
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(long jobId) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject(PRIVATE_ID, jobId);
        Bson query = Filters.eq(PRIVATE_ID, jobId);
//        Job job = parseJob(jobCollection.<DBObject>find(query, new BasicDBObject("visits", true), null));

        Job job = getJob(jobId, new QueryOptions(MongoDBCollection.INCLUDE, "visits")).first();
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

    @Deprecated
    @Override
    public QueryResult modifyJob(long jobId, ObjectMap parameters) throws CatalogDBException {
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
    public QueryResult<JobAcl> getJobAcl(long jobId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
        checkJobId(jobId);
        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, jobId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), members));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACLS.key()));

        List<JobAcl> jobAcl = null;
        QueryResult<Document> aggregate = jobCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        Job job = jobConverter.convertToDataModelType(aggregate.first());

        if (job != null) {
            jobAcl = job.getAcls();
        }

        return endQuery("get job Acl", startTime, jobAcl);
    }

    @Override
    public QueryResult<JobAcl> setJobAcl(long jobId, JobAcl acl, boolean override) throws CatalogDBException {
        long startTime = startQuery();
        checkJobId(jobId);
        long studyId = getStudyIdByJobId(jobId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, acl.getUsers());

        // If there are groups in acl.getUsers(), we will obtain all the users belonging to the groups and will check if any of them
        // already have permissions on its own.
        Map<String, List<String>> groups = new HashMap<>();
        Set<String> users = new HashSet<>();
        for (String member : acl.getUsers()) {
            if (member.startsWith("@")) {
                Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();
                groups.put(group.getId(), group.getUserIds());
            } else {
                users.add(member);
            }
        }
        if (groups.size() > 0) {
            // Check if any user already have permissions set on their own.
            for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
                QueryResult<JobAcl> jobAcl = getJobAcl(jobId, entry.getValue());
                if (jobAcl.getNumResults() > 0) {
                    throw new CatalogDBException("Error when adding permissions in job. At least one user in " + entry.getKey()
                            + " has already defined permissions for job " + jobId);
                }
            }
        }

        // Check if any of the users in the set of users also belongs to any introduced group. In that case, we will remove the user
        // because the group will be given the permission.
        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            for (String userId : entry.getValue()) {
                if (users.contains(userId)) {
                    users.remove(userId);
                }
            }
        }

        // Create the definitive list of members that will be added in the acl
        List<String> members = new ArrayList<>(users.size() + groups.size());
        members.addAll(users.stream().collect(Collectors.toList()));
        members.addAll(groups.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        acl.setUsers(members);

        // Check if the members of the new acl already have some permissions set
        QueryResult<JobAcl> jobAcls = getJobAcl(jobId, acl.getUsers());
        if (jobAcls.getNumResults() > 0 && override) {
            Set<String> usersSet = new HashSet<>(acl.getUsers().size());
            usersSet.addAll(acl.getUsers().stream().collect(Collectors.toList()));

            List<String> usersToOverride = new ArrayList<>();
            for (JobAcl jobAcl : jobAcls.getResult()) {
                for (String member : jobAcl.getUsers()) {
                    if (usersSet.contains(member)) {
                        // Add the user to the list of users that will be taken out from the Acls.
                        usersToOverride.add(member);
                    }
                }
            }

            // Now we remove the old permissions set for the users that already existed so the permissions are overriden by the new ones.
            unsetJobAcl(jobId, usersToOverride, Collections.emptyList());
        } else if (jobAcls.getNumResults() > 0 && !override) {
            throw new CatalogDBException("setJobAcl: " + jobAcls.getNumResults() + " of the members already had an Acl set. If you "
                    + "still want to set the Acls for them and remove the old one, please use the override parameter.");
        }

        // Append the users to the existing acl.
        List<String> permissions = acl.getPermissions().stream().map(JobAcl.JobPermissions::name).collect(Collectors.toList());

        // Check if the permissions found on acl already exist on job id
        Document queryDocument = new Document(PRIVATE_ID, jobId);
        if (permissions.size() > 0) {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", permissions.size()).append("$all", permissions));
        } else {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", 0));
        }

        Bson update;
        if (jobCollection.count(queryDocument).first() > 0) {
            // Append the users to the existing acl.
            update = new Document("$addToSet", new Document("acls.$.users", new Document("$each", acl.getUsers())));
        } else {
            queryDocument = new Document(PRIVATE_ID, jobId);
            // Push the new acl to the list of acls.
            update = new Document("$push", new Document(QueryParams.ACLS.key(), getMongoDBDocument(acl, "JobAcl")));

        }

        QueryResult<UpdateResult> updateResult = jobCollection.update(queryDocument, update, null);
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("setJobAcl: An error occurred when trying to share job " + jobId + " with other members.");
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.ACLS.key());
        Job job = jobConverter.convertToDataModelType(jobCollection.find(queryDocument, queryOptions).first());

        return endQuery("setJobAcl", startTime, job.getAcls());
    }

    @Override
    public void unsetJobAcl(long jobId, List<String> members, List<String> permissions) throws CatalogDBException {
        checkJobId(jobId);

        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyIdByJobId(jobId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, jobId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update;
            if (permissions.size() == 0) {
                update = new Document("$pull", new Document("acls.$.users", member));
            } else {
                update = new Document("$pull", new Document("acls.$.permissions", new Document("$in", permissions)));
            }
            QueryResult<UpdateResult> updateResult = jobCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetJobAcl: An error occurred when trying to stop sharing job " + jobId
                        + " with other " + member + ".");
            }
        }

        // Remove possible jobAcls that might have permissions defined but no users
        Bson queryBson = new Document(QueryParams.ID.key(), jobId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        jobCollection.update(queryBson, update, null);
    }

    @Override
    public void unsetJobAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_STUDY_ID, studyId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update = new Document("$pull", new Document("acls.$.users", member));
            jobCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }

        // Remove possible JobAcls that might have permissions defined but no users
        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        jobCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
    }

    @Override
    public long getStudyIdByJobId(long jobId) throws CatalogDBException {
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
    public QueryResult<Long> extractFilesFromJobs(List<Long> fileIds) throws CatalogDBException {
        long startTime = startQuery();
        long numResults;

        // Check for input
        Query query = new Query(QueryParams.INPUT.key(), fileIds);
        Bson bsonQuery = parseQuery(query);
        Bson update = new Document("$pull", new Document(QueryParams.INPUT.key(), new Document("$in", fileIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        numResults = jobCollection.update(bsonQuery, update, multi).first().getModifiedCount();

        // Check for output
        query = new Query(QueryParams.OUTPUT.key(), fileIds);
        bsonQuery = parseQuery(query);
        update = new Document("$pull", new Document(QueryParams.OUTPUT.key(), new Document("$in", fileIds)));
        numResults += jobCollection.update(bsonQuery, update, multi).first().getModifiedCount();

        return endQuery("Extract files from jobs", startTime, Collections.singletonList(numResults));
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
    public boolean experimentExists(long experimentId) {
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
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
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
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
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

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.USER_ID.key(), QueryParams.TOOL_NAME.key(), QueryParams.DATE.key(),
                QueryParams.DESCRIPTION.key(), QueryParams.OUTPUT_ERROR.key(), QueryParams.COMMAND_LINE.key(),
                QueryParams.OUT_DIR_ID.key(), QueryParams.ERROR.key(), QueryParams.ERROR_DESCRIPTION.key(),
        };
        filterStringParams(parameters, jobParameters, acceptedParams);

//        Map<String, Class<? extends Enum>> acceptedEnums =
//                Collections.singletonMap((QueryParams.JOB_STATUS.key()), Job.JobStatusEnum.class);
//        filterEnumParams(parameters, jobParameters, acceptedEnums);
        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            jobParameters.put(QueryParams.STATUS_STATUS.key(), parameters.get(QueryParams.STATUS_STATUS.key()));
            jobParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }


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
    public QueryResult<Job> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        checkJobId(id);
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Long> update = update(query, parameters);
        if (update.getResult().isEmpty() || update.first() != 1) {
            throw new CatalogDBException("Could not update job " + id);
        }
        return endQuery("Update user", startTime, get(query, null));
    }


    public QueryResult<Job> clean(int id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        QueryResult<Job> jobQueryResult = get(query, null);
        if (jobQueryResult.getResult().size() == 1) {
            QueryResult<DeleteResult> delete = jobCollection.remove(parseQuery(query), null);
            if (delete.getResult().size() == 0) {
                throw CatalogDBException.newInstance("Job id '{}' has not been deleted", id);
            }
        } else {
            throw CatalogDBException.idNotFound("Job id '{}' does not exist (or there are too many)", id);
        }
        return jobQueryResult;
    }

    @Override
    public QueryResult<Job> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkJobId(id);

        // Check the status of the job
        Query query = new Query(QueryParams.ID.key(), id);
        Job job = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.STATUS_STATUS.key())).first();
        switch (job.getStatus().getStatus()) {
            case Job.JobStatus.DELETED:
            case Job.JobStatus.REMOVED:
                throw new CatalogDBException("The job {" + id + "} was already " + job.getStatus().getStatus());
            case Job.JobStatus.PREPARED:
            case Job.JobStatus.RUNNING:
            case Job.JobStatus.QUEUED:
                throw new CatalogDBException("The job {" + id + "} is " + job.getStatus().getStatus()
                        + ". Please, stop the job before deleting it.");
            case Job.JobStatus.DONE:
            case Job.JobStatus.ERROR:
            case Job.JobStatus.READY:
            default:
                break;
        }

        // If we don't find the force and the keep output files parameter, we check first if the job has any active file.
        if ((!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE))
                && (!queryOptions.containsKey(KEEP_OUTPUT_FILES) || !queryOptions.getBoolean(KEEP_OUTPUT_FILES))) {
            checkCanDelete(id);
        }

        query = new Query(CatalogFileDBAdaptor.QueryParams.JOB_ID.key(), id);
        if (queryOptions.containsKey(KEEP_OUTPUT_FILES) && queryOptions.getBoolean(KEEP_OUTPUT_FILES)) {
            // unlink the files from the job
            ObjectMap objectMap = new ObjectMap(CatalogFileDBAdaptor.QueryParams.JOB_ID.key(), -1);
            dbAdaptorFactory.getCatalogFileDBAdaptor().update(query, objectMap);
        } else {
            // Remove the files created by the job
            dbAdaptorFactory.getCatalogFileDBAdaptor().delete(query, queryOptions);
        }

        // Change the status of the job to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Status.DELETED);

        return endQuery("Delete job", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        QueryResult<Job> jobQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Job job : jobQueryResult.getResult()) {
            delete(job.getId(), queryOptions);
        }
        return endQuery("Delete job", startTime, Collections.singletonList(jobQueryResult.getNumTotalResults()));
    }


    /**
     * Checks whether the files created by the jobId can be deleted.
     *
     * @param jobId job id.
     * @throws CatalogDBException when any of the files created by the job cannot be deleted.
     */
    private void checkCanDelete(long jobId) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), jobId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.OUTPUT);

        Job job = get(query, queryOptions).first();
        List<Long> output = job.getOutput();
        if (output != null && output.size() > 0) {
            query = new Query(QueryParams.INPUT.key(), job.getOutput());
            if (count(query).first() > 0) {
                throw new CatalogDBException("Cannot delete/remove the job {" + jobId + "}. The files generated by the job have been used "
                        + "as input of other(s). You might want to make use of force/keepOutputFiles parameters.");
            }
        }
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
        query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        return endQuery("Restore jobs", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Job> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkJobId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The job {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore job", startTime, get(query, null));
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
