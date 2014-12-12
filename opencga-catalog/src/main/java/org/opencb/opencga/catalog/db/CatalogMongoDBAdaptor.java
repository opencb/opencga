package org.opencb.opencga.catalog.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor extends CatalogDBAdaptor {

    private static final String USER_COLLECTION = "user";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String METADATA_COLLECTION = "metadata";

    private static final String METADATA_OBJECT_ID = "METADATA";

    private final MongoDataStoreManager mongoManager;
    private final MongoCredential credentials;
    private final DataStoreServerAddress dataStoreServerAddress;
    private MongoDataStore db;

    private MongoDBCollection metaCollection;
    private MongoDBCollection userCollection;
    private MongoDBCollection fileCollection;
    private MongoDBCollection sampleCollection;
    private MongoDBCollection jobCollection;

    //    private static final Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static ObjectReader jsonFileReader;
    private static ObjectReader jsonUserReader;
    private static ObjectReader jsonJobReader;
    private static ObjectReader jsonProjectReader;
    private static ObjectReader jsonStudyReader;
    private static ObjectReader jsonAnalysisReader;
    private static ObjectReader jsonSampleReader;

    private Properties catalogProperties;

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonFileReader = jsonObjectMapper.reader(File.class);
        jsonUserReader = jsonObjectMapper.reader(User.class);
        jsonJobReader = jsonObjectMapper.reader(Job.class);
        jsonProjectReader = jsonObjectMapper.reader(Project.class);
        jsonStudyReader = jsonObjectMapper.reader(Study.class);
        jsonAnalysisReader = jsonObjectMapper.reader(Job.class);
        jsonSampleReader = jsonObjectMapper.reader(Sample.class);
    }

    public CatalogMongoDBAdaptor(DataStoreServerAddress dataStoreServerAddress, MongoCredential credentials)
            throws CatalogDBException {
        super();
        this.dataStoreServerAddress = dataStoreServerAddress;
        this.mongoManager = new MongoDataStoreManager(this.dataStoreServerAddress.getHost(), this.dataStoreServerAddress.getPort());
        this.credentials = credentials;
        //catalogProperties = Config.getAccountProperties();

        logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);

        connect();
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(credentials.getSource());
        if(db == null){
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        metaCollection = db.getCollection(METADATA_COLLECTION);
        userCollection = db.getCollection(USER_COLLECTION);
        fileCollection = db.getCollection(FILE_COLLECTION);
        sampleCollection = db.getCollection(SAMPLE_COLLECTION);
        jobCollection = db.getCollection(JOB_COLLECTION);

        //If "metadata" document doesn't exist, create.
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_OBJECT_ID));
        if(queryResult.getResult().get(0) == 0){
            try {
                DBObject metadataObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(new Metadata()));
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject);
            } catch (MongoException.DuplicateKey e){
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            } catch (JsonProcessingException e) {
                logger.error("Metadata json parse error", e);
            }
            //Set indexes
//            BasicDBObject unique = new BasicDBObject("unique", true);
//            nativeUserCollection.createIndex(new BasicDBObject("id", 1), unique);
//            nativeFileCollection.createIndex(BasicDBObjectBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndex(new BasicDBObject("id", 1), unique);
        }
    }

    @Override
    public void disconnect(){
        mongoManager.close(db.getDatabaseName());
    }


    /**
     Auxiliary query methods
     */
    private int getNewProjectId()  {return CatalogMongoDBUtils.getNewAutoIncrementId("projectCounter", metaCollection);}
    private int getNewStudyId()    {return CatalogMongoDBUtils.getNewAutoIncrementId("studyCounter", metaCollection);}
    private int getNewFileId()     {return CatalogMongoDBUtils.getNewAutoIncrementId("fileCounter", metaCollection);}
    //    private int getNewAnalysisId() {return CatalogMongoDBUtils.getNewAutoIncrementId("analysisCounter");}
    private int getNewJobId()      {return CatalogMongoDBUtils.getNewAutoIncrementId("jobCounter", metaCollection);}
    private int getNewToolId()      {return CatalogMongoDBUtils.getNewAutoIncrementId("toolCounter", metaCollection);}
    private int getNewSampleId()   {return CatalogMongoDBUtils.getNewAutoIncrementId("sampleCounter", metaCollection);}


    private void checkParameter(Object param, String name) throws CatalogDBException {
        if (param == null) {
            throw new CatalogDBException("Error: parameter '" + name + "' is null");
        }
        if(param instanceof String) {
            if(param.equals("") || param.equals("null")) {
                throw new CatalogDBException("Error: parameter '" + name + "' is empty or it values 'null");
            }
        }
    }

    /** **************************
     * User methods
     * ***************************
     */

    @Override
    public boolean checkUserCredentials(String userId, String sessionId) {
        return false;
    }

    @Override
    public boolean userExists(String userId){
        QueryResult<Long> count = userCollection.count(new BasicDBObject("id", userId));
        long l = count.getResult().get(0);
        return l != 0;
    }

    @Override
    public QueryResult<User> createUser(String userId, String userName, String email, String password,
                                        String organization) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

        if(userExists(userId)) {
            throw new CatalogDBException("User {id:\"" + userId + "\"} already exists");
        }
        return null;

    }

    @Override
    public QueryResult<User> insertUser(User user) throws CatalogDBException {
        checkParameter(user, "user");
        long startTime = startQuery();

        if(userExists(user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        user.setLastActivity(TimeUtils.getTimeMillis());
        DBObject userDBObject;
        try {
            userDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("create user failed at parsing user " + user.getId());
        }
        userDBObject.put("_id", user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("User {id:\""+user.getId()+"\"} already exists");
        }

        String errorMsg = (insert.getErrorMsg() != null) ? insert.getErrorMsg() : "";
        for (Project p : projects) {
            String projectErrorMsg = createProject(user.getId(), p).getErrorMsg();
            if(projectErrorMsg != null && !projectErrorMsg.isEmpty()){
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

        //Get the inserted user.
        List<User> result = getUser(user.getId(), null, "").getResult();

        return endQuery("insertUser", startTime, result, errorMsg, null);
    }

    /**
     * TODO: delete user from:
     *      project acl and owner
     *      study acl and owner
     *      file acl and creator
     *      job userid
     * also, delete his:
     *      projects
     *      studies
     *      analysesS
     *      jobs
     *      files
     */
    @Override
    public QueryResult<Integer> deleteUser(String userId) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

//        WriteResult id = nativeUserCollection.remove(new BasicDBObject("id", userId));
        WriteResult wr = userCollection.remove(new BasicDBObject("id", userId)).getResult().get(0);
        if (wr.getN() == 0) {
            throw new CatalogDBException("user {id:" + userId + "} not found");
        } else {
            return endQuery("Delete user", startTime, Arrays.asList(wr.getN()));
        }
    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException, IOException {
        checkParameter(userId, "userId");
        checkParameter(password, "password");

        long startTime = startQuery();

        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
        if(count.getResult().get(0) == 0){
            throw new CatalogDBException("Bad user or password");
        } else {

            QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
            if (countSessions.getResult().get(0) != 0) {
                throw new CatalogDBException("Already logged");
            } else {
                BasicDBObject id = new BasicDBObject("id", userId);
                BasicDBObject updates = new BasicDBObject(
                        "$push", new BasicDBObject(
                        "sessions", JSON.parse(jsonObjectWriter.writeValueAsString(session))
                )
                );
                userCollection.update(id, updates, false, false);

                ObjectMap resultObjectMap = new ObjectMap();
                resultObjectMap.put("sessionId", session.getId());
                resultObjectMap.put("userId", userId);
                return endQuery("Login", startTime, Arrays.asList(resultObjectMap));
            }
        }
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogDBException {
        long startTime = startQuery();

        String userIdBySessionId = getUserIdBySessionId(sessionId);
        if(userIdBySessionId.isEmpty()){
            return endQuery("logout", startTime, null, "", "Session not found");
        }
        if(userIdBySessionId.equals(userId)){
            userCollection.update(
                    new BasicDBObject("sessions.id", sessionId),
                    new BasicDBObject("$set", new BasicDBObject("sessions.$.logout", TimeUtils.getTime())),
                    false,
                    false);

        } else {
            throw new CatalogDBException("UserId mismatches with the sessionId");
        }

        return endQuery("Logout", startTime);
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if(countSessions.getResult().get(0) != 0){
            throw new CatalogDBException("Error, sessionID already exists");
        }
        String userId = "anonymous_" + session.getId();
        User user = new User(userId, "Anonymous", "", "", "", User.ROLE_ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous;
        try {
            anonymous = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("Error parsing user to json", e);
        }
        anonymous.put("_id", user.getId());

        try {
            userCollection.insert(anonymous);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("Anonymous user {id:\""+user.getId()+"\"} already exists");
        }

        ObjectMap resultObjectMap = new ObjectMap();
        resultObjectMap.put("sessionId", session.getId());
        resultObjectMap.put("userId", userId);
        return endQuery("Login as anonymous", startTime, Arrays.asList(resultObjectMap));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) throws CatalogDBException {
        long startTime = startQuery();
        String userId = "anonymous_" + sessionId;
        logout(userId, sessionId);
        deleteUser(userId);
        return endQuery("Logout anonymous", startTime);
    }

    @Override
    public QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException {
        long startTime = startQuery();
        DBObject query = new BasicDBObject("id", userId);
//        query.put("lastActivity", new BasicDBObject("$ne", lastActivity));
        QueryResult result = userCollection.find(query, options);
        User user = parseUser(result);

        if(user == null){
            throw new CatalogDBException("User  {id:" + userId + "} not found");
        }
        if(user.getLastActivity() != null && user.getLastActivity().equals(lastActivity)) {
            return endQuery("Get user", startTime);
        } else {
            return endQuery("Get user", startTime, Arrays.asList(user));
        }
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("password", oldPassword);
        BasicDBObject fields = new BasicDBObject("password", newPassword);
        BasicDBObject action = new BasicDBObject("$set", fields);
        QueryResult<WriteResult> update = userCollection.update(query, action, false, false);
        if(update.getResult().get(0).getN() == 0){  //0 query matches.
            throw new CatalogDBException("Bad user or password");
        }
        return endQuery("Change Password", startTime, update);
    }

    @Override
    public QueryResult changeEmail(String userId, String newEmail) throws CatalogDBException {
        return modifyUser(userId, new ObjectMap("email", newEmail));
    }

    @Override
    public void updateUserLastActivity(String userId) throws CatalogDBException {
        modifyUser(userId, new ObjectMap("lastActivity", TimeUtils.getTimeMillis()));
    }

    @Override
    public QueryResult modifyUser(String userId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> userParameters = new HashMap<>();

        String[] acceptedParams = {"name", "email", "organization", "lastActivity", "role", "status"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                userParameters.put(s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    userParameters.put(s, anInt);
                }
            }
        }
        Map<String, Object> attributes = parameters.getMap("attributes");
        if(attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                userParameters.put("attributes." + entry.getKey(), entry.getValue());
            }
        }
        Map<String, Object> configs = parameters.getMap("configs");
        if(configs != null) {
            for (Map.Entry<String, Object> entry : configs.entrySet()) {
                userParameters.put("configs." + entry.getKey(), entry.getValue());
            }
        }

        if(!userParameters.isEmpty()) {
            QueryResult<WriteResult> update = userCollection.update(
                    new BasicDBObject("id", userId),
                    new BasicDBObject("$set", userParameters), false, false);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
                throw new CatalogDBException("User {id:'" + userId + "'} not found");
            }
        }

        return endQuery("Modify user", startTime);
    }

    @Override
    public QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("email", email);
        BasicDBObject fields = new BasicDBObject("password", newCryptPass);
        BasicDBObject action = new BasicDBObject("$set", fields);
        QueryResult<WriteResult> update = userCollection.update(query, action, false, false);
        if(update.getResult().get(0).getN() == 0){  //0 query matches.
            throw new CatalogDBException("Bad user or email");
        }
        return endQuery("Reset Password", startTime, update);
    }

    @Override
    public QueryResult getSession(String userId, String sessionId) throws CatalogDBException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUserIdBySessionId(String sessionId){
        QueryResult id = userCollection.find(
                BasicDBObjectBuilder
                        .start("sessions.id", sessionId)
                        .append("sessions.logout", "").get(),
                null,
                null,
                new BasicDBObject("id", true));

        if (id.getNumResults() != 0) {
            return (String) ((DBObject) id.getResult().get(0)).get("id");
        } else {
            return "";
        }
    }


    /**
     * Project methods
     * ***************************
     */

    @Override
    public boolean projectExists(int projectId) {
        QueryResult<Long> count = userCollection.count(new BasicDBObject("projects.id", projectId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Project> createProject(String userId, Project project) throws CatalogDBException {
        long startTime = startQuery();

        List<Study> studies = project.getStudies();
        if(studies == null) {
            studies = Collections.emptyList();
        }
        project.setStudies(Collections.<Study>emptyList());


        // Check if project.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start("id", userId)
                .append("projects.alias", project.getAlias())
                .get();
        QueryResult<Long> count = userCollection.count(countQuery);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }
//        if(getProjectId(userId, project.getAlias()) >= 0){
//            throw new CatalogManagerException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
//        }

        //Generate json
        int projectId = getNewProjectId();
        project.setId(projectId);
        DBObject query = new BasicDBObject("id", userId);
        query.put("projects.alias", new BasicDBObject("$ne", project.getAlias()));
        DBObject update;
        try {
            update = new BasicDBObject("$push", new BasicDBObject ("projects", JSON.parse(jsonObjectWriter.writeValueAsString(project))));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("Create project failed at parsing project " + project.getAlias());
        }

        //Update object
        QueryResult<WriteResult> queryResult = userCollection.update(query, update, false, false);

        if (queryResult.getResult().get(0).getN() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = createStudy(project.getId(), study).getErrorMsg();
            if(studyErrorMsg != null && !studyErrorMsg.isEmpty()){
                errorMsg += ", " + study.getAlias() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = getProject(project.getId(), null).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult<Project> getProject(String userId, String projectAlias) throws CatalogDBException {
        int projectId = getProjectId(userId, projectAlias);
        return getProject(projectId, null);
    }

    @Override
    public QueryResult<Project> getProject(int projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = new BasicDBObject(
                "projects",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("id", projectId)
                )
        );
        QueryResult result = userCollection.find(query, options, null, projection);
        User user = parseUser(result);
        if(user == null || user.getProjects().isEmpty()) {
            throw new CatalogDBException("Project {id:" + projectId + "} not found");
        }
        return endQuery("Get project", startTime, user.getProjects());
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject pull = new BasicDBObject("$pull",
                new BasicDBObject("projects",
                        new BasicDBObject("id", projectId)));

        QueryResult<WriteResult> update = userCollection.update(query, pull, false, false);
        List<Integer> deletes = new LinkedList<>();
        if (update.getResult().get(0).getN() == 0) {
            throw new CatalogDBException("project {id:" + projectId + "} not found");
        } else {
            deletes.add(update.getResult().get(0).getN());
            return endQuery("delete project", startTime, deletes);
        }
    }

    @Override
    public QueryResult<Project> getAllProjects(String userId) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject("projects", true);
        projection.put("_id", false);
        QueryResult result = userCollection.find(query, null, null, projection);

        User user = parseUser(result);
        return endQuery(
                "User projects list", startTime,
                user.getProjects());
    }


    /**
     db.user.update(
     {
     "projects.id" : projectId,
     "projects.alias" : {
     $ne : newAlias
     }
     },
     {
     $set:{
     "projects.$.alias":newAlias
     }
     })
     */
    @Override
    public QueryResult renameProjectAlias(int projectId, String newProjectAlias) throws CatalogDBException {
        long startTime = startQuery();
//        String projectOwner = getProjectOwner(projectId);
//
//        int collisionProjectId = getProjectId(projectOwner, newProjectAlias);
//        if (collisionProjectId != -1) {
//            throw new CatalogManagerException("Couldn't rename project alias, alias already used in the same user");
//        }

        QueryResult<Project> projectResult = getProject(projectId, null); // if projectId doesn't exist, an exception is raised
        Project project = projectResult.getResult().get(0);

        String oldAlias = project.getAlias();
        project.setAlias(newProjectAlias);

        DBObject query = BasicDBObjectBuilder
                .start("projects.id", projectId)
                .append("projects.alias", new BasicDBObject("$ne", newProjectAlias))    // check that any other project in the user has the new name
                .get();
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject("projects.$.alias", newProjectAlias));

        QueryResult<WriteResult> result = userCollection.update(query, update, false, false);
        if (result.getResult().get(0).getN() == 0) {    //Check if the the study has been inserted
            throw new CatalogDBException("Project {alias:\"" + newProjectAlias+ "\"} already exists");
        }
        return endQuery("rename project alias", startTime, result);
    }

    @Override
    public QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        if (!projectExists(projectId)) {
            throw new CatalogDBException("Project {id:"+projectId+"} does not exist");
        }
        BasicDBObject projectParameters = new BasicDBObject();

        String[] acceptedParams = {"name", "creationDate", "description", "organization", "status", "lastActivity"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                projectParameters.put("projects.$."+s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    projectParameters.put(s, anInt);
                }
            }
        }
        Map<String, Object> attributes = parameters.getMap("attributes");
        if(attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                projectParameters.put("projects.$.attributes."+entry.getKey(), entry.getValue());
            }
//            projectParameters.put("projects.$.attributes", attributes);
        }

        if(!projectParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject("projects.id", projectId);
            BasicDBObject updates = new BasicDBObject("$set", projectParameters);
            QueryResult<WriteResult> updateResult = userCollection.update(query, updates, false, false);
            if(updateResult.getResult().get(0).getN() == 0){
                throw new CatalogDBException("Project {id:"+projectId+"} does not exist");
            }
        }
        return endQuery("Modify project", startTime);
    }

    @Override
    public int getProjectId(String userId, String projectAlias) throws CatalogDBException {
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", projectAlias)
                        .append("id", userId).get(),
                null,
                null,
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", projectAlias))).get()
        );
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            return user.getProjects().get(0).getId();
        }
    }

    @Override
    public String getProjectOwnerId(int projectId) throws CatalogDBException {
        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

        if(result.getResult().isEmpty()){
            throw new CatalogDBException("Project {id:"+projectId+"} not found");
        } else {
            return result.getResult().get(0).get("id").toString();
        }
    }

    public Acl getFullProjectAcl(int projectId, String userId) throws CatalogDBException {
        QueryResult<Project> project = getProject(projectId, null);
        if (project.getNumResults() != 0) {
            List<Acl> acl = project.getResult().get(0).getAcl();
            for (Acl acl1 : acl) {
                if (userId.equals(acl1.getUserId())) {
                    return acl1;
                }
            }
        }
        return null;
    }
    /**
     * db.user.aggregate(
     * {"$match": {"projects.id": 2}},
     * {"$project": {"projects.acl":1, "projects.id":1}},
     * {"$unwind": "$projects"},
     * {"$match": {"projects.id": 2}},
     * {"$unwind": "$projects.acl"},
     * {"$match": {"projects.acl.userId": "jmmut"}}).pretty()
     */
    @Override
    public QueryResult<Acl> getProjectAcl(int projectId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject match1 = new BasicDBObject("$match", new BasicDBObject("projects.id", projectId));
        DBObject project = new BasicDBObject("$project", BasicDBObjectBuilder
                .start("_id", false)
                .append("projects.acl", true)
                .append("projects.id", true).get());
        DBObject unwind1 = new BasicDBObject("$unwind", "$projects");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("projects.id", projectId));
        DBObject unwind2 = new BasicDBObject("$unwind", "$projects.acl");
        DBObject match3 = new BasicDBObject("$match", new BasicDBObject("projects.acl.userId", userId));

        List<DBObject> operations = new LinkedList<>();
        operations.add(match1);
        operations.add(project);
        operations.add(unwind1);
        operations.add(match2);
        operations.add(unwind2);
        operations.add(match3);
        QueryResult aggregate = userCollection.aggregate(null, operations, null);

        List<Acl> acls = new LinkedList<>();
        if (aggregate.getNumResults() != 0) {
            Object aclObject = ((DBObject) ((DBObject) aggregate.getResult().get(0)).get("projects")).get("acl");
            Acl acl;
            try {
                acl = jsonObjectMapper.reader(Acl.class).readValue(aclObject.toString());
                acls.add(acl);
            } catch (IOException e) {
                throw new CatalogDBException("get Project ACL: error parsing ACL");
            }
        }
        return endQuery("get project ACL", startTime, acls);
    }

    @Override
    public QueryResult setProjectAcl(int projectId, Acl newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject;
        try {
            newAclObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(newAcl));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("could not put ACL: parsing error");
        }

        List<Acl> projectAcls = getProjectAcl(projectId, userId).getResult();
        DBObject query = new BasicDBObject("projects.id", projectId);
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("projects.$.acl", newAclObject));
        if (!projectAcls.isEmpty()) {  // ensure that there is no acl for that user in that project. pull
            DBObject pull = new BasicDBObject("$pull", new BasicDBObject("projects.$.acl", new BasicDBObject("userId", userId)));
            userCollection.update(query, pull, false, false);
        }
        //Put study
        QueryResult pushResult = userCollection.update(query, push, false, false);
        return endQuery("Set project acl", startTime, pushResult);
    }


    public QueryResult<File> searchProject(QueryOptions query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBList filters = new BasicDBList();

        if(query.containsKey("name")){
            filters.add(new BasicDBObject("name", query.getString("name")));
        }
        if(query.containsKey("like")){
            filters.add(new BasicDBObject("name", new BasicDBObject("$regex", query.getString("like"))));
        }
        if(query.containsKey("startsWith")){
            filters.add(new BasicDBObject("name", new BasicDBObject("$regex", "^"+query.getString("startsWith"))));
        }
        if(query.containsKey("alias")){
            filters.add(new BasicDBObject("alias", query.getString("alias")));
        }
//        if(query.containsKey("maxSize")){
//            filters.add(new BasicDBObject("size", new BasicDBObject("$lt", query.getInt("maxSize"))));
//        }
//        if(query.containsKey("minSize")){
//            filters.add(new BasicDBObject("size", new BasicDBObject("$gt", query.getInt("minSize"))));
//        }
        if(query.containsKey("startDate")){
            filters.add(new BasicDBObject("creationDate", new BasicDBObject("$lt", query.getString("startDate"))));
        }
        if(query.containsKey("endDate")){
            filters.add(new BasicDBObject("creationDate", new BasicDBObject("$gt", query.getString("endDate"))));
        }

        QueryResult<DBObject> queryResult = fileCollection.find(new BasicDBObject("$and", filters), options);

        List<File> files = parseFiles(queryResult);

        return endQuery("Search Proyect", startTime, files);
    }

    /**
     * Study methods
     * ***************************
     */

    @Override
    public boolean studyExists(int studyId) {
        QueryResult<Long> count = userCollection.count(new BasicDBObject("projects.studies.id", studyId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Study> createStudy(int projectId, Study study) throws CatalogDBException {
        long startTime = startQuery();
        if(projectId < 0){
            throw new CatalogDBException("Project {id:"+projectId+"} not found");
        }

        // Check if study.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start()
                .append("projects", new BasicDBObject("$elemMatch", BasicDBObjectBuilder
                        .start("id", projectId)
                        .append("alias", study.getAlias()).get()))
                .get();
        QueryResult<Long> queryResult = userCollection.count(countQuery);
        if (queryResult.getResult().get(0) != 0) {
            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }
//        if (getStudyId(projectId, study.getAlias()) >= 0) {
//            throw new CatalogManagerException("Study {alias:\"" + study.getAlias() + "\"} already exists");
//        }
        study.setId(getNewStudyId());

        List<File> files = study.getFiles();
        study.setFiles(Collections.<File>emptyList());

        // Analysis has been removed, now Jobs are stored directly into Study
//        List<Analysis> analyses = study.getAnalyses();
//        study.setAnalyses(Collections.<Analysis>emptyList());
        List<Job> jobs = study.getJobs();
        study.setJobs(Collections.<Job>emptyList());

        DBObject query = new BasicDBObject();
//        query.put("projects.id", projectId);
//        query.put("projects.studies.alias", new BasicDBObject("$ne", study.getAlias()));
        query.put("projects", new BasicDBObject(
                "$elemMatch", BasicDBObjectBuilder.start()
                .append("id", projectId)
                .append("studies.alias", new BasicDBObject("$ne", study.getAlias()))
                .get()
        ));

        DBObject studyObject = null;
        try {
            studyObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(study));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("Error parsing study.", e);
        }
        DBObject update = new BasicDBObject("$push", new BasicDBObject(
                "projects.$.studies", studyObject ));
        QueryResult<WriteResult> updateResult = userCollection.update(query, update, false, false);

        //Check if the the study has been inserted
        if (updateResult.getResult().get(0).getN() == 0) {
            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        String errorMsg = updateResult.getErrorMsg() != null? updateResult.getErrorMsg() : "";
//        for(Analysis analysis : analyses){
//            String analysisErrorMsg;
//            analysisErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
//            if(analysisErrorMsg != null && !analysisErrorMsg.isEmpty()){
//                errorMsg += analysis.getAlias() + ":" + analysisErrorMsg + ", ";
//            }
//        }

        for (File file : files) {
            String fileErrorMsg = createFileToStudy(study.getId(), file).getErrorMsg();
            if(fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg +=  file.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (Job job : jobs) {
//            String jobErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
            String jobErrorMsg = createJob(study.getId(), job).getErrorMsg();
            if(jobErrorMsg != null && !jobErrorMsg.isEmpty()){
                errorMsg += job.getName() + ":" + jobErrorMsg + ", ";
            }
        }

        List<Study> studyList = getStudy(study.getId(), null).getResult();
        return endQuery("Create Study", startTime, studyList, errorMsg, null);

    }

    @Override
    public QueryResult<Study> getAllStudies(int projectId) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = BasicDBObjectBuilder
                .start(new BasicDBObject(
                                "projects",
                                new BasicDBObject(
                                        "$elemMatch",
                                        new BasicDBObject("id", projectId)
                                )
                        )
                ).append("projects.studies", true).get();

        QueryResult queryResult = endQuery("get project", startTime, userCollection.find(query, null, null, projection));

        User user = parseUser(queryResult);
        if(user == null || user.getProjects().isEmpty()) {
            throw new CatalogDBException("Project {id:"+projectId+"} not found");
        }
        List<Study> studies = user.getProjects().get(0).getStudies();
        for (Study study : studies) {
            study.setDiskUsage(getDiskUsageByStudy(study.getId()));
//            study.setAnalyses(getAllAnalysis(study.getId()).getResult());
            //TODO: append files
            //TODO: append jobs
        }
        return endQuery("Get all studies", startTime, studies);
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        //TODO append files in the studies

        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject projection = BasicDBObjectBuilder
                .start(new BasicDBObject(
                                "projects",
                                new BasicDBObject(
                                        "$elemMatch",
                                        new BasicDBObject("studies.id", studyId)
                                )
                        )
                ).append("projects.studies", true).get();

        QueryResult result = userCollection.find(query, options, null, projection);
        QueryResult queryResult = endQuery("get study", startTime, result);

        List<Study> studies;
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            throw new CatalogDBException("Study {id:" + studyId + "} not found");
        } else {
            studies = user.getProjects().get(0).getStudies();
            Study study = null;
            for (Study st : studies) {
                if (st.getId() == studyId) {
                    study = st;
                }
            }
            if (study != null) {
                study.setDiskUsage(getDiskUsageByStudy(study.getId()));
//                study.setAnalyses(getAllAnalysis(studyId).getResult());
            }
            // TODO study.setfiles
            studies = new LinkedList<>();
            studies.add(study);
        }
        //queryResult.setResult(studies);
        return endQuery("Get Study", startTime, studies);

    }

    @Override
    public QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudyName) throws CatalogDBException {
        throw new CatalogDBException("Unsupported operation");
    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException {
//        long startTime = startQuery();
//
//        QueryResult studyResult = getStudy(studyId, sessionId);
        return null;
    }

    @Override
    public void updateStudyLastActivity(int studyId) throws CatalogDBException {
        modifyStudy(studyId, new ObjectMap("lastActivity", TimeUtils.getTime()));
    }

    @Override
    public QueryResult modifyStudy(int studyId, ObjectMap params) throws CatalogDBException {

        long startTime = startQuery();
        int projectIdByStudyId = getProjectIdByStudyId(studyId);
        QueryResult<Study> studyResult = getStudy(studyId, null);
        if(studyResult.getResult().isEmpty()){
            throw new CatalogDBException("Can't find study");
        }

        Study study = studyResult.getResult().get(0);
        study.setName(params.getString("name", study.getName()));
        study.setType(Study.StudyType.valueOf(params.getString("type", study.getType().name())));
        study.setCreatorId(params.getString("creatorId", study.getCreatorId()));
        study.setCreationDate(params.getString("creationDate", study.getCreationDate()));
        study.setDescription(params.getString("description", study.getDescription()));
        study.setUri(params.get("uri", URI.class, study.getUri()));
        study.setStatus(params.getString("status", study.getStatus()));
        study.setDiskUsage(params.getInt("diskUsage", (int) study.getDiskUsage())); //Fixme: may lost precision
        study.setCipher(params.getString("cipher", study.getCipher()));
        study.setAcl(params.getListAs("acl", Acl.class, study.getAcl()));
        study.setLastActivity(params.getString("lastActivity", study.getLastActivity()));
        study.getStats().putAll(params.getMap("stats", Collections.<String, Object>emptyMap()));
        study.getAttributes().putAll(params.getMap("attributes", Collections.<String, Object>emptyMap()));

        DBObject dbObject = null;
        try {
            dbObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(study));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("Fail parse study to json");
        }
//        dbObject.putAll(parameters);

        BasicDBObject query = new BasicDBObject("projects.id", projectIdByStudyId);
        //Pull study
        userCollection.update(query, new BasicDBObject("$pull", new BasicDBObject("projects.$.studies", new BasicDBObject("id", studyId))), false, false);
        //Put study
        userCollection.update(query, new BasicDBObject("$push", new BasicDBObject("projects.$.studies", dbObject)), false, false);

        return endQuery("Modify Study", startTime);
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteStudy(String userId, String projectAlias, String studyAlias) throws CatalogDBException {
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        return deleteStudy(studyId);
    }

    @Override
    public QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject pull =  new BasicDBObject("$pull", new BasicDBObject("projects.$.studies", new BasicDBObject("id", studyId)));
        QueryResult<WriteResult> update = userCollection.update(query, pull, false, false);
        List<Integer> deletes = new LinkedList<>();
        if (update.getResult().get(0).getN() == 0) {
            throw new CatalogDBException("study {id:" + studyId + "} not found");
        } else {
            deletes.add(update.getResult().get(0).getN());
            return endQuery("delete study", startTime, deletes);
        }
    }

    @Override
    public int getStudyId(int projectId, String studyAlias) throws CatalogDBException {
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.id", projectId)
                        .append("projects.studies.alias", studyAlias)
                                // .append("id", userId)
                        .get(),
                null,
                null,
                BasicDBObjectBuilder
                        .start("projects.studies.id", true)
                        .append("projects.studies.alias", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("id", projectId)))
                        .get()
        );
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            for (Study s : user.getProjects().get(0).getStudies()) {
                if(s.getAlias().equals(studyAlias)){
                    return s.getId();
                }
            }
            return -1;
        }
    }

    @Override
    public int getStudyId(String userId, String projectAlias, String studyAlias) throws CatalogDBException {
        return getStudyId(getProjectId(userId, projectAlias), studyAlias);
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogDBException {
        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject projection = new BasicDBObject("projects.id", "true");
        projection.put("projects", new BasicDBObject("$elemMatch", new BasicDBObject("studies.id", studyId)));
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

        User user = parseUser(result);
        if (user != null && !user.getProjects().isEmpty()) {
            return user.getProjects().get(0).getId();
        } else {
            throw new CatalogDBException("Study {id:"+studyId+"} not found");
        }
    }

    @Override
    public String getStudyOwnerId(int studyId) throws CatalogDBException {
        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

        User user = parseUser(result);
        if (user != null) {
            return user.getId();
        } else {
            throw new CatalogDBException("Study {id:" + studyId + "} not found");
        }
    }

    @Override
    public QueryResult<Acl> getStudyAcl(int studyId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        List<Acl> acls = new LinkedList<>();
        QueryResult<Study> studyQuery = getStudy(studyId, null);
        List<Acl> acl = studyQuery.getResult().get(0).getAcl();
        for (Acl acl1 : acl) {
            if (userId.equals(acl1.getUserId())) {
                acls.add(acl1);
            }
        }
        return endQuery("get study ACL", startTime, acls);
    }

    @Override
    public QueryResult setStudyAcl(int studyId, Acl newAcl) throws CatalogDBException {
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        List<Acl> studyAcl = getStudy(studyId, null).getResult().get(0).getAcl();

        boolean exists = false;
        for (Acl acl : studyAcl) {
            if (acl.getUserId().equals(newAcl.getUserId())) {
                acl.setUserId(newAcl.getUserId());
                acl.setDelete(newAcl.isDelete());
                acl.setExecute(newAcl.isExecute());
                acl.setRead(newAcl.isRead());
                acl.setWrite(newAcl.isWrite());
                exists = true;
            }
        }
        if (!exists) {
            studyAcl.add(newAcl);
        }

        ObjectMap objectMap = new ObjectMap();
        objectMap.put("acl", studyAcl);
        modifyStudy(studyId, objectMap);
        return getStudyAcl(studyId, userId);
    }

    /**
     * File methods
     * ***************************
     */

    @Override
    public QueryResult<File> createFileToStudy(String userId, String projectAlias, String studyAlias, File file) throws CatalogDBException {
        long startTime = startQuery();

        int studyId = getStudyId(userId, projectAlias, studyAlias);
        if(studyId < 0){
            throw new CatalogDBException("Study {alias:"+studyAlias+"} does not exists");
        }
        QueryResult<File> fileToStudy = createFileToStudy(studyId, file);
        return endQuery("Create file", startTime, fileToStudy);
    }

    @Override
    public QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogDBException {
        long startTime = startQuery();

        String ownerId = getStudyOwnerId(studyId);
        if(ownerId == null || ownerId.isEmpty()) {
            throw new CatalogDBException("StudyID " + studyId + " not found");
        }
        BasicDBObject query = new BasicDBObject("studyId", studyId);
        query.put("path", file.getPath());
        QueryResult<Long> count = fileCollection.count(query);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
        }


        int newFileId = getNewFileId();
        file.setId(newFileId);
        if(file.getOwnerId() == null) {
            file.setOwnerId(ownerId);
        }
        DBObject fileDBObject;
        try {
            fileDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(file));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
        fileDBObject.put("studyId", studyId);
        try {
            fileCollection.insert(fileDBObject);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
        }

        return endQuery("Create file", startTime, Arrays.asList(file));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteFile(String userId, String projectAlias, String studyAlias, String path) throws CatalogDBException, IOException {
        return deleteFile(getFileId(userId, projectAlias, studyAlias, path));
    }

    @Override
    public QueryResult<Integer> deleteFile(int studyId, String path) throws CatalogDBException {
        return deleteFile(getFileId(studyId, path));
    }

    @Override
    public QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException {
        long startTime = startQuery();

        WriteResult id = fileCollection.remove(new BasicDBObject("id", fileId)).getResult().get(0);
        List<Integer> deletes = new LinkedList<>();
        if(id.getN() == 0) {
            throw new CatalogDBException("file {id:" + fileId + "} not found");
        } else {
            deletes.add(id.getN());
            return endQuery("delete file", startTime, deletes);
        }
    }

    @Override
    public QueryResult deleteFilesFromStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult deleteFilesFromStudy(int studyId, String studyAlias, String sessionId) throws CatalogDBException {
        return null;
    }

    @Override
    public int getFileId(String userId, String projectAlias, String studyAlias, String path) throws CatalogDBException {
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        return getFileId(studyId, path);
    }

    @Override
    public int getFileId(int studyId, String path) throws CatalogDBException {

        DBObject query = BasicDBObjectBuilder
                .start("studyId", studyId)
                .append("path", path).get();
        BasicDBObject fields = new BasicDBObject("id", true);
        QueryResult queryResult = fileCollection.find(query, null, null, fields);
        File file = parseFile(queryResult);
        if(file != null) {
            return file.getId();
        } else {
            return -1;
        }
    }

    @Override
    public QueryResult<File> getAllFiles(int studyId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult queryResult = fileCollection.find( new BasicDBObject("studyId", studyId), null, null, null);
        List<File> files = parseFiles(queryResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> folderResult = fileCollection.find( new BasicDBObject("id", folderId), null, null, null);

        File folder = parseFile(folderResult);
        if (!folder.getType().equals(File.TYPE_FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Object studyId = folderResult.getResult().get(0).get("studyId");

        BasicDBObject query = new BasicDBObject("studyId", studyId);
        query.put("path", new BasicDBObject("$regex", "^" + folder.getPath() + "[^/]+/?$"));
        QueryResult filesResult = fileCollection.find(query, null, null, null);
        List<File> files = parseFiles(filesResult);

        return endQuery("Get all files", startTime, files);
    }


    @Override
    public QueryResult<File> getFile(int fileId) throws CatalogDBException {
        return getFile(fileId, null);
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult queryResult = fileCollection.find( new BasicDBObject("id", fileId), options);

        File file = parseFile(queryResult);
        if(file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw new CatalogDBException("File {id:"+fileId+"} not found");
        }
    }

    @Override
    public QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, String path, String status) throws CatalogDBException, IOException {
        int fileId = getFileId(userId, projectAlias, studyAlias, path);
        return setFileStatus(fileId, status);
    }

    @Override
    public QueryResult setFileStatus(int studyId, String path, String status) throws CatalogDBException, IOException {
        int fileId = getFileId(studyId, path);
        return setFileStatus(fileId, status);
    }

    @Override
    public QueryResult setFileStatus(int fileId, String status) throws CatalogDBException, IOException {
        long startTime = startQuery();
//        BasicDBObject query = new BasicDBObject("id", fileId);
//        BasicDBObject updates = new BasicDBObject("$set",
//                new BasicDBObject("status", status));
//        QueryResult<WriteResult> update = fileCollection.update(query, updates, false, false);
//        if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
//            throw new CatalogManagerException("File {id:"+fileId+"} not found");
//        }
//        return endQuery("Set file status", startTime);
        return endQuery("Set file status", startTime, modifyFile(fileId, new ObjectMap("status", status)));
    }

    @Override
    public QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {"type", "format", "bioformat", "uriScheme", "description", "status"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                fileParameters.put(s, parameters.getString(s));
            }
        }

        String[] acceptedLongParams = {"diskUsage"};
        for (String s : acceptedLongParams) {
            if(parameters.containsKey(s)) {
                fileParameters.put(s, parameters.getLong(s));
            }
        }

        String[] acceptedIntParams = {"jobId"};
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                fileParameters.put(s, parameters.getInt(s));
            }
        }

        Map<String, Object> attributes = parameters.getMap("attributes");
        if(attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                fileParameters.put("attributes." + entry.getKey(), entry.getValue());
            }
        }
        Map<String, Object> stats = parameters.getMap("stats");
        if(stats != null) {
            for (Map.Entry<String, Object> entry : stats.entrySet()) {
                fileParameters.put("stats." + entry.getKey(), entry.getValue());
            }
        }

        if(!fileParameters.isEmpty()) {
            QueryResult<WriteResult> update = fileCollection.update(new BasicDBObject("id", fileId),
                    new BasicDBObject("$set", fileParameters), false, false);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
                throw new CatalogDBException("File {id:"+fileId+"} not found");
            }
        }

        return endQuery("Modify file", startTime);
    }

//    @Override
//    public QueryResult setIndexFile(int fileId, String backend, Index index) throws CatalogManagerException {
//        long startTime = startQuery();
//
//
//        fileCollection.update(
//                new BasicDBObject("id", fileId),
//                new BasicDBObject("$pull",
//                        new BasicDBObject("indices",
//                                new BasicDBObject("backend",
//                                        backend
//                                )
//                        )
//                ), false, false);
//        if(index != null){
//            try {
//                fileCollection.update(
//                        new BasicDBObject("id", fileId),
//                        new BasicDBObject("$push",
//                                new BasicDBObject("indices",
//                                        JSON.parse(jsonObjectWriter.writeValueAsString(index))
//                                )
//                        ), false, false);
//            } catch (JsonProcessingException e) {
//                throw new CatalogManagerException(e);
//            }
//        }
//
//        return endQuery("Set index file", startTime);
//    }

    /**
     * @param filePath assuming 'pathRelativeToStudy + name'
     */
    @Override
    public QueryResult<WriteResult> renameFile(int fileId, String filePath) throws CatalogDBException {
        long startTime = startQuery();

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        File file = getFile(fileId, null).getResult().get(0);
        if (file.getType().equals(File.TYPE_FOLDER)) {
            throw new UnsupportedOperationException("Renaming folders still not supported");  // no renaming folders. it will be a future feature
        }

        int studyId = getStudyIdByFileId(fileId);
        int collisionFileId = getFileId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        BasicDBObject query = new BasicDBObject("id", fileId);
        BasicDBObject set = new BasicDBObject("$set", BasicDBObjectBuilder
                .start("name", fileName)
                .append("path", filePath).get());
        QueryResult<WriteResult> update = fileCollection.update(query, set, false, false);
        if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
            throw new CatalogDBException("File {id:" + fileId + "} not found");
        }
        return endQuery("rename file", startTime, update);
    }


    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
        DBObject query = new BasicDBObject("id", fileId);
        DBObject projection = new BasicDBObject("studyId", "true");
        QueryResult<DBObject> result = fileCollection.find(query, null, null, projection);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get("studyId");
        } else {
            throw new CatalogDBException("Study not found");
        }
    }

    @Override
    public String getFileOwnerId(int fileId) throws CatalogDBException {
        QueryResult<File> fileQueryresult = getFile(fileId);
        if(fileQueryresult == null || fileQueryresult.getResult() == null || fileQueryresult.getResult().isEmpty()) {
            throw new CatalogDBException("File {id: " + fileId + "} not found");
        }
        return fileQueryresult.getResult().get(0).getOwnerId();
//        int studyId = getStudyIdByFileId(fileId);
//        return getStudyOwnerId(studyId);
    }

    private int getDiskUsageByStudy(int studyId){
        List<DBObject> operations = Arrays.<DBObject>asList(
                new BasicDBObject(
                        "$match",
                        new BasicDBObject(
                                "studyId",
                                studyId
                                //new BasicDBObject("$in",studyIds)
                        )
                ),
                new BasicDBObject(
                        "$group",
                        BasicDBObjectBuilder
                                .start("_id", "$studyId")
                                .append("diskUsage",
                                        new BasicDBObject(
                                                "$sum",
                                                "$diskUsage"
                                        )).get()
                )
        );
        QueryResult<DBObject> aggregate = (QueryResult<DBObject>) fileCollection.aggregate(null, operations, null);
        if(aggregate.getNumResults() == 1){
            Object diskUsage = aggregate.getResult().get(0).get("diskUsage");
            if(diskUsage instanceof Integer){
                return (Integer)diskUsage;
            } else {
                return Integer.parseInt(diskUsage.toString());
            }
        } else {
            return 0;
        }
    }

    /**
     * query: db.file.find({id:2}, {acl:{$elemMatch:{userId:"jcoll"}}, studyId:1})
     */
    @Override
    public QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject projection = BasicDBObjectBuilder
                .start("acl",
                        new BasicDBObject("$elemMatch",
                                new BasicDBObject("userId", userId)))
                .append("_id", false)
                .get();

        QueryResult queryResult = fileCollection.find(new BasicDBObject("id", fileId), null, null, projection);
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("getFileAcl: There is no file with fileId = " + fileId);
        }
        List<Acl> acl = parseFile(queryResult).getAcl();
        return endQuery("get file acl", startTime, acl);
    }

    @Override
    public QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject;
        try {
            newAclObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(newAcl));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("could not put ACL: parsing error");
        }

        List<Acl> acls = getFileAcl(fileId, userId).getResult();
        DBObject match;
        DBObject updateOperation;
        if (acls.isEmpty()) {  // there is no acl for that user in that file. push
            match = new BasicDBObject("id", fileId);
            updateOperation = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            match = BasicDBObjectBuilder
                    .start("id", fileId)
                    .append("acl.userId", userId).get();
            updateOperation = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        QueryResult update = fileCollection.update(match, updateOperation, false, false);
        return endQuery("set file acl", startTime);
    }

    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBList filters = new BasicDBList();

        if(query.containsKey("name")){
            filters.add(new BasicDBObject("name", query.getString("name")));
        }
        if(query.containsKey("type")){
            filters.add(new BasicDBObject("type", query.getString("type")));
        }
        if(query.containsKey("path")){
            filters.add(new BasicDBObject("path", query.getString("path")));
        }
        if(query.containsKey("bioformat")){
            filters.add(new BasicDBObject("bioformat", query.getString("bioformat")));
        }
        if(query.containsKey("maxSize")){
            filters.add(new BasicDBObject("size", new BasicDBObject("$lt", query.getInt("maxSize"))));
        }
        if(query.containsKey("minSize")){
            filters.add(new BasicDBObject("size", new BasicDBObject("$gt", query.getInt("minSize"))));
        }
        if(query.containsKey("startDate")){
            filters.add(new BasicDBObject("creationDate", new BasicDBObject("$lt", query.getString("startDate"))));
        }
        if(query.containsKey("endDate")){
            filters.add(new BasicDBObject("creationDate", new BasicDBObject("$gt", query.getString("endDate"))));
        }
        if(query.containsKey("like")){
            filters.add(new BasicDBObject("name", new BasicDBObject("$regex", query.getString("like"))));
        }
        if(query.containsKey("startsWith")){
            filters.add(new BasicDBObject("name", new BasicDBObject("$regex", "^"+query.getString("startsWith"))));
        }
        if(query.containsKey("directory")){
            filters.add(new BasicDBObject("path", new BasicDBObject("$regex", "^"+query.getString("directory")+"[^/]+/?$")));
        }
        if(query.containsKey("studyId")){
            filters.add(new BasicDBObject("studyId", query.getInt("studyId")));
        }
        if(query.containsKey("status")){
            filters.add(new BasicDBObject("status", query.getString("status")));
        }

        QueryResult<DBObject> queryResult = fileCollection.find(new BasicDBObject("$and", filters), options);

        List<File> files = parseFiles(queryResult);

        return endQuery("Search File", startTime, files);
    }


    /**
     * Analysis methods
     * ***************************
     */
//    public boolean analysisExists(int analysisId) {
//        QueryResult count = userCollection.count(new BasicDBObject("analyses.id", analysisId));
//        long l = (Long) count.getResult().get(0);
//        return l != 0;
//    }
//
//    @Override
//    public QueryResult<Analysis> getAllAnalysis(String userId, String projectAlias, String studyAlias) throws CatalogManagerException {
//        long startTime = startQuery();
//        int studyId = getStudyId(userId, projectAlias, studyAlias);
//        if (studyId < 0) {
//            throw new CatalogManagerException("Study not found");
//        } else {
//            QueryResult<Analysis> allAnalysis = getAllAnalysis(studyId);
//            return endQuery("Get all Analysis", startTime, allAnalysis);
//        }
//    }
//
//    /**
//     *  aggregation: db.user.aggregate([{"$match":{"analyses.studyId": 8}}, {$project:{analyses:1}}, {$unwind:"$analyses"}, {$match:{"analyses.studyId": 8}}, {$group:{"_id":"$studyId", analyses:{$push:"$analyses"}}}]).pretty()
//     */
//    @Override
//    public QueryResult<Analysis> getAllAnalysis(int studyId) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        DBObject match1 = new BasicDBObject("$match", new BasicDBObject("analyses.studyId", studyId));
//        DBObject project = new BasicDBObject("$project", new BasicDBObject("analyses", 1));
//        DBObject unwind = new BasicDBObject("$unwind", "$analyses");
//        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("analyses.studyId", studyId));
//        DBObject group = new BasicDBObject(
//                "$group",
//                BasicDBObjectBuilder
//                        .start("_id", "$studyId")
//                        .append("analyses", new BasicDBObject("$push", "$analyses")).get());
//
//        List<DBObject> operations = new LinkedList<>();
//        operations.add(match1);
//        operations.add(project);
//        operations.add(unwind);
//        operations.add(match2);
//        operations.add(group);
//        QueryResult result = userCollection.aggregate(null, operations, null);
//
//        List<Analysis> analyses = new LinkedList<>();
//        try {
//            if (result.getNumResults() != 0) {
//                Study study = jsonStudyReader.readValue(result.getResult().get(0).toString());
//                analyses = study.getAnalyses();
//                // TODO fill analyses with jobs
//            }
//            return endQuery("get all analyses", startTime, analyses);
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new CatalogManagerException("Failed to parse mongo : " + e.getMessage());
//        }
//    }
//
//    /**
//     * query: db.user.find({"analyses.alias":"analysis1Alias", "analyses.studyId":8}
//     * , {"analyses":{$elemMatch:{studyId:8,alias:"analysis1Alias"}},"analyses.id":1}).pretty()
//     */
//    @Override
//    public int getAnalysisId(int studyId, String analysisAlias) throws CatalogManagerException {
//        DBObject elem = new BasicDBObject("$elemMatch", BasicDBObjectBuilder
//                .start("studyId", studyId)
//                .append("alias", analysisAlias).get()
//        );
//        DBObject match = new BasicDBObject("analyses", elem);
//        DBObject projection = BasicDBObjectBuilder
//                .start("analyses", elem)
//                .append("_id", false).get();
//        QueryResult result = userCollection.find(match, null, null, projection);
//        List<Analysis> analyses = parseAnalyses(result);
//        return analyses.size() == 0? -1: analyses.get(0).getId();
//    }
//
//    @Override
//    public QueryResult<Analysis> getAnalysis(int analysisId) throws CatalogManagerException {
//        String queryId = "get analysis";
//        long startTime = startQuery();
//        DBObject query = new BasicDBObject("analyses.id", analysisId);
//        DBObject projection = new BasicDBObject(
//                "analyses",
//                new BasicDBObject(
//                        "$elemMatch",
//                        new BasicDBObject("id", analysisId)
//                )
//        );
//
//        QueryResult result = userCollection.find(query, null, null, projection);
//        Study study;
//        List<Analysis> analyses = new LinkedList<>();
//        if (result.getNumResults() != 0) {
//            try {
//                study = jsonStudyReader.readValue(result.getResult().get(0).toString());
//            } catch (IOException e) {
//                throw new CatalogManagerException("Error parsing analysis", e);
//            }
//            analyses = study.getAnalyses();
//        }
//        return endQuery(queryId, startTime, analyses);
//    }
//
//    @Override
//    public QueryResult<Analysis> createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis) throws CatalogManagerException {
//        int studyId = getStudyId(userId, projectAlias, studyAlias);
//        if (studyId < 0) {
//            throw new CatalogManagerException("Study not found");
//        } else {
//            return createAnalysis(studyId, analysis);
//        }
//    }
//
//    @Override
//    public QueryResult<Analysis> createAnalysis(int studyId, Analysis analysis) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        // Check if analysis.alias already exists.
//        QueryResult<Long> count = userCollection.count(
//                new BasicDBObject("analyses",
//                        new BasicDBObject("$elemMatch", BasicDBObjectBuilder
//                                .start("studyId", studyId)
//                                .append("alias", analysis.getAlias()).get()
//                        )
//                )
//        );
//        if(count.getResult().get(0) != 0) {
//            throw new CatalogManagerException("Analysis alias " + analysis.getAlias() + " already exists in study " + studyId);
//        }
//
//        // complete and push Analysis: id, studyId, jobs...
//        analysis.setId(getNewAnalysisId());
//
//        List<Job> jobs = analysis.getJobs();
//        analysis.setJobs(Collections.<Job>emptyList());
//
//        DBObject query = new BasicDBObject("projects.studies.id", studyId);
//        DBObject analysisObject;
//        try {
//            analysisObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(analysis));
//        } catch (JsonProcessingException e) {
//            throw new CatalogManagerException("analysis " + analysis.getAlias() + " could not be parsed into json", e);
//        }
//        analysisObject.put("studyId", studyId);
//        DBObject update = new BasicDBObject("$push", new BasicDBObject("analyses", analysisObject));
//        QueryResult updateResult = userCollection.update(query, update, false, false);
//
//        // fill other collections: jobs
//        if (jobs == null) {
//            jobs = Collections.<Job>emptyList();
//        }
//
//        // TODO for (j:jobs) createJob(j)
////        for (Job job : jobs) {
////        }
//
//        return endQuery("Create Analysis", startTime, getAnalysis(analysis.getId()));
//
//    }
//
//    @Override
//    public QueryResult modifyAnalysis(int analysisId, ObjectMap parameters) throws CatalogManagerException {
//        long startTime = startQuery();
//        if(!analysisExists(analysisId)){
//            throw new CatalogManagerException("Analysis {id:"+analysisId+"} does not exist");
//        }
//
//        BasicDBObject analysisParameters = new BasicDBObject();
//
//        String[] acceptedParams = {"name", "date", "description"};
//        for (String s : acceptedParams) {
//            if(parameters.containsKey(s)) {
//                analysisParameters.put("analyses.$." + s, parameters.getString(s));
//            }
//        }
//        Map<String, Object> attributes = parameters.getMap("attributes");
//        if(attributes != null) {
//            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
//                analysisParameters.put("analyses.$.attributes." + entry.getKey(), entry.getValue());
//            }
////            analysisParameters.put("projects.$.attributes", attributes);
//        }
//
//        if(!analysisParameters.isEmpty()) {
//            BasicDBObject query = new BasicDBObject("analyses.id", analysisId);
//            BasicDBObject updates = new BasicDBObject("$set", analysisParameters);
//            QueryResult<WriteResult> updateResult = userCollection.update(query, updates, false, false);
//            if(updateResult.getResult().get(0).getN() == 0){
//                throw new CatalogManagerException("Analysis {id:"+analysisId+"} does not exist");
//            }
//        }
//        return endQuery("Modify analysis", startTime);
//    }
//
//    @Override
//    public int getStudyIdByAnalysisId(int analysisId) throws CatalogManagerException {
//        DBObject query = new BasicDBObject("analyses.id", analysisId);
//        DBObject returnFields = BasicDBObjectBuilder
//                .start("analyses.studyId", true)
//                .append("analyses",
//                    new BasicDBObject(
//                            "$elemMatch",
//                            new BasicDBObject("id", analysisId)
//                    ))
//                .get();
//        QueryResult id = userCollection.find(query, null, null, returnFields);
//
//        if (id.getNumResults() != 0) {
//            List<DBObject> analyses = (List<DBObject>) ((DBObject) id.getResult().get(0)).get("analyses");
//            if(analyses.isEmpty()) {
//                return -1;
//            } else {
//                return Integer.parseInt(analyses.get(0).get("studyId").toString());
//            }
//        } else {
//            return -1;
//        }
//    }
//
//    @Override
//    public String getAnalysisOwner(int analysisId) throws CatalogManagerException {
//        DBObject query = new BasicDBObject("analyses.id", analysisId);
//        DBObject returnFields = new BasicDBObject("id", analysisId);
//        QueryResult id = userCollection.find(query, null, null, returnFields);
//
//        User user = parseUser(id);
//        if (user != null) {
//            return user.getId();
//        } else {
//            throw new CatalogManagerException("Study {id:"+analysisId+"} not found");
//        }
//    }

    /**
     * Job methods
     * ***************************
     */

    @Override
    public boolean jobExists(int jobId) {
        QueryResult<Long> count = jobCollection.count(new BasicDBObject("id", jobId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Job> createJob(int studyId, Job job) throws CatalogDBException {
        long startTime = startQuery();

//        if (!analysisExists(analysisId)) {
//            throw new CatalogManagerException("Analysis {id:" + analysisId + "} does not exist");
//        }

        //TODO Check StudyId exists
        if(studyId < 0) {
            throw new CatalogDBException("Study {id:" + studyId + "} does not exist");
        }

        int jobId = getNewJobId();
        job.setId(jobId);

        DBObject jobObject;
        try {
            jobObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(job));
            jobObject.put("_id", jobId);
            jobObject.put("studyId", studyId);
            QueryResult insertResult = jobCollection.insert(jobObject); //TODO: Check results.get(0).getN() != 0
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("job " + job + " could not be parsed into json", e);
        }

        return endQuery("Create Job", startTime, getJob(jobId));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteJob(int jobId) throws CatalogDBException {
        long startTime = startQuery();

        WriteResult id = jobCollection.remove(new BasicDBObject("id", jobId)).getResult().get(0);
        List<Integer> deletes = new LinkedList<>();
        if (id.getN() == 0) {
            throw new CatalogDBException("job {id: " + jobId + "} not found");
        } else {
            deletes.add(id.getN());
            return endQuery("delete job", startTime, deletes);
        }
    }

    @Override
    public QueryResult<Job> getJob(int jobId) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult queryResult = jobCollection.find(new BasicDBObject("id", jobId), null);
        Job job = parseJob(queryResult);
        if(job != null) {
            return endQuery("Get job", startTime, Arrays.asList(job));
        } else {
            throw new CatalogDBException("Job {id:" + jobId + "} not found");
        }
    }

    @Override
    public QueryResult<Job> getAllJobs(int studyId) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult queryResult = jobCollection.find(new BasicDBObject("studyId", studyId), null);
        List<Job> jobs = parseJobs(queryResult);
        return endQuery("Get all jobs", startTime, jobs);
    }

    @Override
    public String getJobStatus(int jobId, String sessionId) throws CatalogDBException, IOException {   // TODO remove?
        return null;
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", jobId);
        Job job = parseJob(jobCollection.find(query, null, null, new BasicDBObject("visits", true)));
        int visits;
        if (job != null) {
            visits = job.getVisits()+1;
            BasicDBObject set = new BasicDBObject("$set", new BasicDBObject("visits", visits));
            jobCollection.update(query, set, false, false);
        } else {
            throw new CatalogDBException("Job {id: " + jobId + "} not found");
        }
        return endQuery("Inc visits", startTime, Arrays.asList(new ObjectMap("visits", visits)));
    }

    @Override
    public QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> jobParameters = new HashMap<>();

        String[] acceptedParams = {"name", "userId", "toolName", "date", "description", "outputError", "commandLine", "status", "outdir"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                jobParameters.put(s, parameters.getString(s));
            }
        }
        String[] acceptedIntParams = {"visits"};
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                jobParameters.put(s, parameters.getInt(s));
            }
        }

        String[] acceptedLongParams = {"startTime", "endTime", "diskUsage"};
        for (String s : acceptedLongParams) {
            if(parameters.containsKey(s)) {
                Object value = parameters.get(s);    //TODO: Add "getLong" to "ObjectMap"
                if(value instanceof Long) {
                    jobParameters.put(s, value);
                }
            }
        }

        String[] acceptedListParams = {"output"};
        for (String s : acceptedListParams) {
            if(parameters.containsKey(s)) {
                jobParameters.put(s, parameters.getListAs(s, Integer.class));
            }
        }

        if(!jobParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject("id", jobId);
            BasicDBObject updates = new BasicDBObject("$set", jobParameters);
//            System.out.println("query = " + query);
//            System.out.println("updates = " + updates);
            QueryResult<WriteResult> update = jobCollection.update(query, updates, false, false);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
                throw new CatalogDBException("Job {id:'" + jobId + "'} not found");
            }
        }
        return endQuery("Modify job", startTime);
    }

    @Override
    public void setJobCommandLine(int jobId, String commandLine, String sessionId) throws CatalogDBException, IOException {   // TODO remove?

    }

    @Override
    public int getStudyIdByJobId(int jobId){
        DBObject query = new BasicDBObject("id", jobId);
        DBObject returnFields = new BasicDBObject("studyId", true);
        QueryResult<DBObject> id = jobCollection.find(query, null, null, returnFields);

        if (id.getNumResults() != 0) {
            int studyId = Integer.parseInt(id.getResult().get(0).get("studyId").toString());
            return studyId;
        } else {
            return -1;
        }
    }

    @Override
    public QueryResult<Job> searchJob(QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject();

        if(options.containsKey("ready")) {
            if(options.getBoolean("ready")) {
                query.put("status", Job.READY);
            } else {
                query.put("status", new BasicDBObject("$ne", Job.READY));
            }
            options.remove("ready");
        }
        query.putAll(options);
//        System.out.println("query = " + query);
        QueryResult queryResult = jobCollection.find(query, null);
        List<Job> jobs = parseJobs(queryResult);
        return endQuery("Search job", startTime, jobs);
    }


    /**
     * Tool methods
     * ***************************
     */

    @Override
    public QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException {
        long startTime = startQuery();

        if (!userExists(userId)) {
            throw new CatalogDBException("User {id:" + userId + "} does not exist");
        }

        // Check if tools.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start("id", userId)
                .append("tools.alias", tool.getAlias())
                .get();
        QueryResult<Long> count = userCollection.count(countQuery);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException( "Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
        }

        tool.setId(getNewToolId());

        DBObject toolObject;
        try {
            toolObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(tool));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException("tool " + tool + " could not be parsed into json", e);
        }

        DBObject query = new BasicDBObject("id", userId);
        query.put("tools.alias", new BasicDBObject("$ne", tool.getAlias()));
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("tools", toolObject));

        //Update object
        QueryResult<WriteResult> queryResult = userCollection.update(query, update, false, false);

        if (queryResult.getResult().get(0).getN() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
        }


        return endQuery("Create Job", startTime, getTool(tool.getId()).getResult());
    }


    public QueryResult<Tool> getTool(int id) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("tools.id", id);
        DBObject projection = new BasicDBObject("tools",
                new BasicDBObject("$elemMatch",
                        new BasicDBObject("id", id)
                )
        );
        QueryResult queryResult = userCollection.find(query, new QueryOptions("include", Arrays.asList("tools")), null, projection);

        if(queryResult.getNumResults() != 1 ) {
            throw new CatalogDBException("Tool {id:" + id + "} no exists");
        }

        User user = parseUser(queryResult);
        return endQuery("Get tool", startTime, user.getTools());
    }

    @Override
    public int getToolId(String userId, String toolAlias) throws CatalogDBException {
        DBObject query = BasicDBObjectBuilder
                .start("id", userId)
                .append("tools.alias", toolAlias).get();
        DBObject projection = new BasicDBObject("tools",
                new BasicDBObject("$elemMatch",
                        new BasicDBObject("alias", toolAlias)
                )
        );

        QueryResult queryResult = userCollection.find(query, null, null, projection);
        if(queryResult.getNumResults() != 1 ) {
            throw new CatalogDBException("Tool {alias:" + toolAlias + "} no exists");
        }
        User user = parseUser(queryResult);
        return user.getTools().get(0).getId();
    }


    /**
     * Experiments methods
     * ***************************
     */

    public boolean experimentExists(int experimentId) {
        return false;
    }

    /**
     * Samples methods
     * ***************************
     */

    public boolean sampleExists(int sampleId) {
        return false;
    }


//    @Override
//    public QueryResult<Tool> searchTool(QueryOptions query, QueryOptions options) {
//        long startTime = startQuery();
//
//        QueryResult queryResult = userCollection.find(new BasicDBObject(options),
//                new QueryOptions("include", Arrays.asList("tools")), null);
//
//        User user = parseUser(queryResult);
//
//        return endQuery("Get tool", startTime, user.getTools());
//    }


    /*
    * Helper methods
    ********************/

    private User parseUser(QueryResult result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonUserReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing user", e);
        }
    }

    private File parseFile(QueryResult result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonFileReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private List<File> parseFiles(QueryResult result) throws CatalogDBException {
        List<File> files = new LinkedList<>();
        try {
            for (Object o : result.getResult()) {
                files.add(jsonFileReader.<File>readValue(o.toString()));
            }
            return files;
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private Job parseJob(QueryResult result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonJobReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
    }

    private List<Job> parseJobs(QueryResult<DBObject> result) throws CatalogDBException {
        LinkedList<Job> jobs = new LinkedList<>();
        try {
            for (Object object : result.getResult()) {
                jobs.add(jsonJobReader.<Job>readValue(object.toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
        return jobs;
    }

    /**
     * must receive in the result: [{analyses:[{},...]}].
     * other keys in the object, besides "analyses", will throw
     * a parse error if they are not present in the Analyses Bean.
     */
//    private List<Analysis> parseAnalyses(QueryResult result) throws CatalogManagerException {
//        List<Analysis> analyses = new LinkedList<>();
//        try {
//            if (result.getNumResults() != 0) {
//                Study study = jsonStudyReader.readValue(result.getResult().get(0).toString());
//                analyses.addAll(study.getAnalyses());
//                // TODO fill analyses with jobs
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new CatalogManagerException("Fail to parse analyses in mongo: " + e.getMessage());
//        }
//        return analyses;
//    }

//    private User appendFilesToUser(User user, List<File> files) {
//
//    }
}
