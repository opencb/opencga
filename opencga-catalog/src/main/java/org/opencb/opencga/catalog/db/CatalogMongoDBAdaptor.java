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
    private static final String STUDY_COLLECTION = "study";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String METADATA_COLLECTION = "metadata";

    static final String METADATA_OBJECT_ID = "METADATA";

    //Keys to foreign objects.
    private static final String _PROJECT_ID = "_projectId";
    private static final String _STUDY_ID = "_studyId";

    private final MongoDataStoreManager mongoManager;
    private final MongoCredential credentials;
//    private final DataStoreServerAddress dataStoreServerAddress;
    private MongoDataStore db;

    private MongoDBCollection metaCollection;
    private MongoDBCollection userCollection;
    private MongoDBCollection studyCollection;
    private MongoDBCollection fileCollection;
    private MongoDBCollection sampleCollection;
    private MongoDBCollection jobCollection;

    //    private static final Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static ObjectReader jsonFileReader;
    private static ObjectReader jsonUserReader;
    private static ObjectReader jsonJobReader;
    private static ObjectReader jsonStudyReader;
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
//        jsonProjectReader = jsonObjectMapper.reader(Project.class);
        jsonStudyReader = jsonObjectMapper.reader(Study.class);
//        jsonAnalysisReader = jsonObjectMapper.reader(Job.class);
        jsonSampleReader = jsonObjectMapper.reader(Sample.class);
    }

    public CatalogMongoDBAdaptor(DataStoreServerAddress dataStoreServerAddress, MongoCredential credentials)
            throws CatalogDBException {
        super();
//        this.dataStoreServerAddress = dataStoreServerAddress;
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort());
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
        studyCollection = db.getCollection(STUDY_COLLECTION);
        fileCollection = db.getCollection(FILE_COLLECTION);
        sampleCollection = db.getCollection(SAMPLE_COLLECTION);
        jobCollection = db.getCollection(JOB_COLLECTION);

        //If "metadata" document doesn't exist, create.
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_OBJECT_ID));
        if(queryResult.getResult().get(0) == 0){
            try {
                DBObject metadataObject = getDbObject(new Metadata(), "Metadata json parse error");
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject);
            } catch (MongoException.DuplicateKey e){
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
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
    private int getNewId()  {return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);}
//    private int getNewProjectId()  {return CatalogMongoDBUtils.getNewAutoIncrementId("projectCounter", metaCollection);}
//    private int getNewStudyId()    {return CatalogMongoDBUtils.getNewAutoIncrementId("studyCounter", metaCollection);}
//    private int getNewFileId()     {return CatalogMongoDBUtils.getNewAutoIncrementId("fileCounter", metaCollection);}
//    //    private int getNewAnalysisId() {return CatalogMongoDBUtils.getNewAutoIncrementId("analysisCounter");}
//    private int getNewJobId()      {return CatalogMongoDBUtils.getNewAutoIncrementId("jobCounter", metaCollection);}
//    private int getNewToolId()      {return CatalogMongoDBUtils.getNewAutoIncrementId("toolCounter", metaCollection);}
//    private int getNewSampleId()   {return CatalogMongoDBUtils.getNewAutoIncrementId("sampleCounter", metaCollection);}


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
        DBObject userDBObject = getDbObject(user, "create user failed at parsing user " + user.getId());
        userDBObject.put("_id", user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("User {id:\""+user.getId()+"\"} already exists");
        }

        String errorMsg = insert.getErrorMsg() != null ? insert.getErrorMsg() : "";
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
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException, JsonProcessingException {
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
                        "sessions", getDbObject(session, "Error parsing session to Json")
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
        User user = new User(userId, "Anonymous", "", "", "", User.Role.ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous = getDbObject(user, "Error parsing user to json");
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
        QueryResult<DBObject> result = userCollection.find(query, options);
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
        int projectId = getNewId();
        project.setId(projectId);
        DBObject query = new BasicDBObject("id", userId);
        query.put("projects.alias", new BasicDBObject("$ne", project.getAlias()));
        DBObject projectDBObject = getDbObject(project, "Create project failed at parsing project " + project.getAlias());
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", projectDBObject));

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
        QueryResult<DBObject> result = userCollection.find(query, options, null, projection);
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
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

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

        //String oldAlias = project.getAlias();
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
        QueryResult<DBObject> queryResult = userCollection.find(
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

        DBObject newAclObject = getDbObject(newAcl, "could not put ACL: parsing error");

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
        QueryResult<Long> count = studyCollection.count(new BasicDBObject("id", studyId));
        return count.getResult().get(0) != 0;
    }

    private boolean studyAliasExists(int projectId, String studyAlias) {
        // Check if study.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start(_PROJECT_ID, projectId)
                .append("alias", studyAlias).get();

        QueryResult<Long> queryResult = studyCollection.count(countQuery);
        return queryResult.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Study> createStudy(int projectId, Study study) throws CatalogDBException {
        long startTime = startQuery();
        if(projectId < 0){
            throw new CatalogDBException("Project {id:"+projectId+"} not found");
        }

        // Check if study.alias already exists.
        if (studyAliasExists(projectId, study.getAlias())) {
            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        //Set new ID
        int newId = getNewId();
        study.setId(newId);

        //Empty nested fields
        List<File> files = study.getFiles();
        study.setFiles(Collections.<File>emptyList());

        List<Job> jobs = study.getJobs();
        study.setJobs(Collections.<Job>emptyList());

        //Create DBObject
        DBObject studyObject = getDbObject(study, "Error parsing study.");
        studyObject.put("_id", newId);

        //Set ProjectId
        studyObject.put(_PROJECT_ID, projectId);

        //Insert
        QueryResult<WriteResult> updateResult = studyCollection.insert(studyObject);

        //Check if the the study has been inserted
//        if (updateResult.getResult().get(0).getN() == 0) {
//            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
//        }

        // Insert nested fields
        String errorMsg = updateResult.getErrorMsg() != null? updateResult.getErrorMsg() : "";

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
        if(!projectExists(projectId)) {
            throw new CatalogDBException("Project {id:"+projectId+"} not found");
        }

        DBObject query = new BasicDBObject(_PROJECT_ID, projectId);

        QueryResult queryResult = endQuery("get project", startTime, studyCollection.find(query, null));

        List<Study> studies = parseStudies(queryResult);
        for (Study study : studies) {
            study.setDiskUsage(getDiskUsageByStudy(study.getId()));
            //TODO: append files
            //TODO: append jobs
        }
        return endQuery("Get all studies", startTime, studies);
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        //TODO append files in the studies
        //TODO: Parse QueryOptions include/exclude
        DBObject query = new BasicDBObject("id", studyId);
        QueryResult result = studyCollection.find(query, options);
        QueryResult queryResult = endQuery("get study", startTime, result);

        List<Study> studies = parseStudies(queryResult);
        if (studies.isEmpty()) {
            throw new CatalogDBException("Study {id:" + studyId + "} not found");
        }

        studies.get(0).setDiskUsage(getDiskUsageByStudy(studyId));

        //queryResult.setResult(studies);
        return endQuery("Get Study", startTime, studies);

    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException {
        //TODO
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
    public QueryResult modifyStudy(int studyId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        if (!studyExists(studyId)) {
            throw new CatalogDBException("Study {id:"+studyId+"} does not exist");
        }
        BasicDBObject studyParameters = new BasicDBObject();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastActivity", "cipher"};
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                studyParameters.put(s, parameters.getString(s));
            }
        }

        String[] acceptedLongParams = {"diskUsage"};
        for (String s : acceptedLongParams) {
            if(parameters.containsKey(s)) {
                long aLong = parameters.getLong(s, Long.MIN_VALUE);
                if(aLong != Long.MIN_VALUE) {
                    studyParameters.put(s, aLong);
                }
            }
        }

        String[] acceptedMapParams = {"attributes", "stats"};
        for (String s : acceptedMapParams) {
            Map<String, Object> map = parameters.getMap(s);
            if(map != null) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    studyParameters.put(s + "." + entry.getKey(), entry.getValue());
                }
            }
        }

        if(parameters.containsKey("type")) {
            Study.Type type = parameters.get("type", Study.Type.class);
            studyParameters.put("type", type);
        }

        if(parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if(!studyParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject("id", studyId);
            BasicDBObject updates = new BasicDBObject("$set", studyParameters);
            QueryResult<WriteResult> updateResult = studyCollection.update(query, updates, false, false);
            if(updateResult.getResult().get(0).getN() == 0){
                throw new CatalogDBException("Study {id:"+studyId+"} does not exist");
            }
        }
        return endQuery("Modify study", startTime);
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject query = new BasicDBObject("id", studyId);
        QueryResult<WriteResult> remove = studyCollection.remove(query);

        List<Integer> deletes = new LinkedList<>();

        if (remove.getResult().get(0).getN() == 0) {
            throw new CatalogDBException("Study {id:" + studyId + "} not found");
        } else {
            deletes.add(remove.getResult().get(0).getN());
            return endQuery("delete study", startTime, deletes);
        }
    }

    @Override
    public int getStudyId(int projectId, String studyAlias) throws CatalogDBException {
        DBObject query = BasicDBObjectBuilder.start(_PROJECT_ID, projectId).append("alias", studyAlias).get();
        BasicDBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> queryResult = studyCollection.find(query, null, projection);
        List<Study> studies = parseStudies(queryResult);
        return studies == null || studies.isEmpty() ? -1 : studies.get(0).getId();
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogDBException {
        DBObject query = new BasicDBObject("id", studyId);
        DBObject projection = new BasicDBObject(_PROJECT_ID, "true");
        QueryResult<DBObject> result = studyCollection.find(query, null, projection);

        if (!result.getResult().isEmpty()) {
            DBObject study = result.getResult().get(0);
            return Integer.parseInt(study.get(_PROJECT_ID).toString());
        } else {
            throw new CatalogDBException("Study {id:" + studyId + "} not found");
        }
    }

    @Override
    public String getStudyOwnerId(int studyId) throws CatalogDBException {
        int projectId = getProjectIdByStudyId(studyId);
        return getProjectOwnerId(projectId);
    }

    @Override
    public QueryResult<Acl> getStudyAcl(int studyId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        BasicDBObject query = new BasicDBObject("id", studyId);
        BasicDBObject projection = new BasicDBObject("acl", new BasicDBObject("$elemMatch", new BasicDBObject("userId", userId)));
        QueryResult<DBObject> dbObjectQueryResult = studyCollection.find(query, null, projection);
        List<Study> studies = parseStudies(dbObjectQueryResult);
        if(studies.isEmpty()) {
            throw new CatalogDBException("Study {id:" + studyId + "} not found");
        } else {
            List<Acl> acl = studies.get(0).getAcl();
            return endQuery("getStudyAcl", startTime, acl);
        }
    }

    @Override
    public QueryResult setStudyAcl(int studyId, Acl newAcl) throws CatalogDBException {
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        String msg = "could not put ACL: parsing error";
        DBObject newAclObject = getDbObject(newAcl, msg);

        BasicDBObject query = new BasicDBObject("id", studyId);
        BasicDBObject pull = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", newAcl.getUserId())));
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        studyCollection.update(query, pull, false, false);
        studyCollection.update(query, push, false, false);

        return getStudyAcl(studyId, userId);
    }


    /**
     * File methods
     * ***************************
     */

    private boolean filePathExists(int studyId, String path) {
        BasicDBObject query = new BasicDBObject(_STUDY_ID, studyId);
        query.put("path", path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogDBException {
        long startTime = startQuery();

        String ownerId = getStudyOwnerId(studyId);
        if(ownerId == null || ownerId.isEmpty()) {
            throw new CatalogDBException("StudyID " + studyId + " not found");
        }

        if(filePathExists(studyId, file.getPath())){
            throw new CatalogDBException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
        }

        //new File Id
        int newFileId = getNewId();
        file.setId(newFileId);
        if(file.getOwnerId() == null) {
            file.setOwnerId(ownerId);
        }
        DBObject fileDBObject = getDbObject(file, "Error parsing file");
        fileDBObject.put(_STUDY_ID, studyId);

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
    public int getFileId(int studyId, String path) throws CatalogDBException {

        DBObject query = BasicDBObjectBuilder
                .start(_STUDY_ID, studyId)
                .append("path", path).get();
        BasicDBObject fields = new BasicDBObject("id", true);
        QueryResult<DBObject> queryResult = fileCollection.find(query, null, null, fields);
        File file = parseFile(queryResult);
        return file != null ? file.getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFiles(int studyId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> queryResult = fileCollection.find( new BasicDBObject(_STUDY_ID, studyId), null, null, null);
        List<File> files = parseFiles(queryResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> folderResult = fileCollection.find( new BasicDBObject("id", folderId), null, null, null);

        File folder = parseFile(folderResult);
        if (!folder.getType().equals(File.Type.FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Object studyId = folderResult.getResult().get(0).get(_STUDY_ID);

        BasicDBObject query = new BasicDBObject(_STUDY_ID, studyId);
        query.put("path", new BasicDBObject("$regex", "^" + folder.getPath() + "[^/]+/?$"));
        QueryResult<DBObject> filesResult = fileCollection.find(query, null, null, null);
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

        QueryResult<DBObject> queryResult = fileCollection.find( new BasicDBObject("id", fileId), options);

        File file = parseFile(queryResult);
        if(file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw new CatalogDBException("File {id:"+fileId+"} not found");
        }
    }

    @Override
    public QueryResult setFileStatus(int fileId, File.Status status) throws CatalogDBException, IOException {
        long startTime = startQuery();
        return endQuery("Set file status", startTime, modifyFile(fileId, new ObjectMap("status", status.toString())));
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

        String[] acceptedMapParams = {"attributes", "stats"};
        for (String s : acceptedMapParams) {
            Map<String, Object> map = parameters.getMap(s);
            if(map != null) {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    fileParameters.put(s + "." + entry.getKey(), entry.getValue());
                }
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


    /**
     * @param filePath assuming 'pathRelativeToStudy + name'
     */
    @Override
    public QueryResult<WriteResult> renameFile(int fileId, String filePath) throws CatalogDBException {
        long startTime = startQuery();

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        File file = getFile(fileId, null).getResult().get(0);
        if (file.getType().equals(File.Type.FOLDER)) {
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
        DBObject projection = new BasicDBObject(_STUDY_ID, "true");
        QueryResult<DBObject> result = fileCollection.find(query, null, null, projection);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get(_STUDY_ID);
        } else {
            throw new CatalogDBException("Study not found");
        }
    }

    @Override
    public String getFileOwnerId(int fileId) throws CatalogDBException {
        QueryResult<File> fileQueryResult = getFile(fileId);
        if(fileQueryResult == null || fileQueryResult.getResult() == null || fileQueryResult.getResult().isEmpty()) {
            throw new CatalogDBException("File {id: " + fileId + "} not found");
        }
        return fileQueryResult.getResult().get(0).getOwnerId();
//        int studyId = getStudyIdByFileId(fileId);
//        return getStudyOwnerId(studyId);
    }

    private int getDiskUsageByStudy(int studyId){
        List<DBObject> operations = Arrays.<DBObject>asList(
                new BasicDBObject(
                        "$match",
                        new BasicDBObject(
                                _STUDY_ID,
                                studyId
                                //new BasicDBObject("$in",studyIds)
                        )
                ),
                new BasicDBObject(
                        "$group",
                        BasicDBObjectBuilder
                                .start("_id", "$"+ _STUDY_ID)
                                .append("diskUsage",
                                        new BasicDBObject(
                                                "$sum",
                                                "$diskUsage"
                                        )).get()
                )
        );
        QueryResult<DBObject> aggregate = fileCollection.aggregate(null, operations, null);
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

        DBObject newAclObject = getDbObject(newAcl, "could not put ACL: parsing error");

        List<Acl> aclList = getFileAcl(fileId, userId).getResult();
        DBObject match;
        DBObject updateOperation;
        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
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
            filters.add(new BasicDBObject(_STUDY_ID, query.getInt("studyId")));
        }
        if(query.containsKey("status")){
            filters.add(new BasicDBObject("status", query.getString("status")));
        }

        QueryResult<DBObject> queryResult = fileCollection.find(new BasicDBObject("$and", filters), options);

        List<File> files = parseFiles(queryResult);

        return endQuery("Search File", startTime, files);
    }

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

        int jobId = getNewId();
        job.setId(jobId);

        DBObject jobObject = getDbObject(job, "job " + job + " could not be parsed into json");
        jobObject.put("_id", jobId);
        jobObject.put(_STUDY_ID, studyId);
        QueryResult insertResult = jobCollection.insert(jobObject); //TODO: Check results.get(0).getN() != 0

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
        QueryResult<DBObject> queryResult = jobCollection.find(new BasicDBObject("id", jobId), null);
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
        QueryResult<DBObject> queryResult = jobCollection.find(new BasicDBObject(_STUDY_ID, studyId), null);
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
        Job job = parseJob(jobCollection.<DBObject>find(query, null, null, new BasicDBObject("visits", true)));
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
        DBObject returnFields = new BasicDBObject(_STUDY_ID, true);
        QueryResult<DBObject> id = jobCollection.find(query, null, null, returnFields);

        if (id.getNumResults() != 0) {
            return Integer.parseInt(id.getResult().get(0).get(_STUDY_ID).toString());
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
                query.put("status", Job.Status.READY);
            } else {
                query.put("status", new BasicDBObject("$ne", Job.Status.READY));
            }
            options.remove("ready");
        }
        query.putAll(options);
//        System.out.println("query = " + query);
        QueryResult<DBObject> queryResult = jobCollection.find(query, null);
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

        tool.setId(getNewId());

        DBObject toolObject = getDbObject(tool, "tool " + tool + " could not be parsed into json");
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
        QueryResult<DBObject> queryResult = userCollection.find(query, new QueryOptions("include", Arrays.asList("tools")), null, projection);

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

        QueryResult<DBObject> queryResult = userCollection.find(query, null, null, projection);
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

    private User parseUser(QueryResult<DBObject> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonUserReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing user", e);
        }
    }

    private List<Study> parseStudies(QueryResult<DBObject> result) throws CatalogDBException {
        List<Study> studies = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                studies.add(jsonStudyReader.<Study>readValue(object.toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing study", e);
        }
        return studies;
    }

    private File parseFile(QueryResult<DBObject> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonFileReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private List<File> parseFiles(QueryResult<DBObject> result) throws CatalogDBException {
        List<File> files = new LinkedList<>();
        try {
            for (DBObject o : result.getResult()) {
                files.add(jsonFileReader.<File>readValue(o.toString()));
            }
            return files;
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private Job parseJob(QueryResult<DBObject> result) throws CatalogDBException {
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
            for (DBObject object : result.getResult()) {
                jobs.add(jsonJobReader.<Job>readValue(object.toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
        return jobs;
    }


    private DBObject getDbObject(Object object, String errorMsg) throws CatalogDBException {
        DBObject dbObject;
        try {
            dbObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            throw new CatalogDBException(errorMsg);
        }
        return dbObject;
    }

}
