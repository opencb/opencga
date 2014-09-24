package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor implements CatalogDBAdaptor {

    private static final String METADATA_COLLECTION = "metadata";
    private static final String USER_COLLECTION = "user";
    private static final String FILE_COLLECTION = "file";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String JOB_COLLECTION = "job";

    private static final String METADATA_OBJECT_ID = "METADATA";

    private final MongoDataStoreManager mongoManager;
    private final MongoCredentials credentials;
    private MongoDataStore db;

    private MongoDBCollection metaCollection;
    private MongoDBCollection userCollection;
    private MongoDBCollection fileCollection;
    private MongoDBCollection sampleCollection;
    private MongoDBCollection jobCollection;
    private DBCollection nativeMetaCollection;
    private DBCollection nativeUserCollection;
    private DBCollection nativeFileCollection;

    private static final Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;
    private static ObjectReader jsonFileReader;
    private static ObjectReader jsonUserReader;
    private static ObjectReader jsonProjectReader;
    private static ObjectReader jsonStudyReader;
    private static ObjectReader jsonSampleReader;

    private Properties catalogProperties;

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonFileReader = jsonObjectMapper.reader(File.class);
        jsonUserReader = jsonObjectMapper.reader(User.class);
        jsonProjectReader = jsonObjectMapper.reader(Project.class);
        jsonStudyReader = jsonObjectMapper.reader(Study.class);
        jsonSampleReader = jsonObjectMapper.reader(Sample.class);
    }

    public CatalogMongoDBAdaptor(MongoCredentials credentials) {
        super();
        this.mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        this.credentials = credentials;
        //catalogProperties = Config.getAccountProperties();

        connect();
    }

    private void connect()  {
        db = mongoManager.get(credentials.getMongoDbName());
        nativeMetaCollection = db.getDb().getCollection(METADATA_COLLECTION);
        nativeFileCollection = db.getDb().getCollection(FILE_COLLECTION);
        nativeUserCollection = db.getDb().getCollection(USER_COLLECTION);

        metaCollection = db.getCollection(METADATA_COLLECTION);
        userCollection = db.getCollection(USER_COLLECTION);
        fileCollection = db.getCollection(FILE_COLLECTION);
        sampleCollection = db.getCollection(SAMPLE_COLLECTION);
        jobCollection = db.getCollection(JOB_COLLECTION);


        //If "metadata" document doesn't exist, create.
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_COLLECTION));
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
            BasicDBObject unique = new BasicDBObject("unique", true);
            nativeUserCollection.createIndex(new BasicDBObject("id", 1), unique);
            nativeFileCollection.createIndex(BasicDBObjectBuilder.start("studyId", 1).append("uri", 1).get(), unique);
        }
    }

    public void disconnect(){
         mongoManager.close(db.getDatabaseName());
    }

    private long startQuery(){
        return System.currentTimeMillis();
    }

    private <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result) throws CatalogManagerException {
        return endQuery(queryId, startTime, result, null, null);
    }

    private <T> QueryResult<T> endQuery(String queryId, long startTime) throws CatalogManagerException {
        return endQuery(queryId, startTime, Collections.<T>emptyList(), null, null);
    }

//    private <T> QueryResult<T> endQuery(String queryId, long startTime, String errorMessage, String warnMessage) throws CatalogManagerException {
//        return endQuery(queryId, startTime, Collections.<T>emptyList(), errorMessage, warnMessage);
//    }
//
//    private <T> QueryResult<T> endQuery(String queryId, long startTime, String errorMessage) throws CatalogManagerException {
//        return endQuery(queryId, startTime, Collections.<T>emptyList(), errorMessage, null);
//    }

    //TODO: Shoud throw error if errorMessage != null?
    private <T> QueryResult<T> endQuery(String queryId, long startTime, QueryResult<T> result) throws CatalogManagerException {
        long end = System.currentTimeMillis();
        result.setId(queryId);
        result.setDbTime((int)(end-startTime));
        if(result.getErrorMsg() != null && !result.getErrorMsg().isEmpty()){
            throw new CatalogManagerException(result.getErrorMsg());
        }
        return result;
    }
    //TODO: Shoud throw error if errorMessage != null?
    private <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result, String errorMessage, String warnMessage) throws CatalogManagerException {
        long end = System.currentTimeMillis();
        if(result == null){
            result = new LinkedList<>();
        }
        int numResults = result.size();
        QueryResult<T> queryResult = new QueryResult<>(queryId, (int) (end - startTime), numResults, numResults, warnMessage, errorMessage, result);
        if(errorMessage != null && !errorMessage.isEmpty()){
            throw new CatalogManagerException(queryResult.getErrorMsg());
        }
        return queryResult;
    }
    /*
        Auxiliary query methods
     */


    private int getNewProjectId()  {return getNewId("projectCounter");}
    private int getNewStudyId()    {return getNewId("studyCounter");}
    private int getNewFileId()     {return getNewId("fileCounter");}
    private int getNewAnalysisId() {return getNewId("analysisCounter");}
    private int getNewJobId()      {return getNewId("jobCounter");}
    private int getNewSampleId()   {return getNewId("sampleCounter");}

    private int getNewId(String field){
        DBObject object = nativeMetaCollection.findAndModify(
                new BasicDBObject("_id", METADATA_OBJECT_ID),  //Query
                new BasicDBObject(field, true),  //Fields
                null,
                false,
                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
                true,
                false
        );
        return (int) Float.parseFloat(object.get(field).toString());
    }



    /**
     * User methods
     * ***************************
     */

    @Override
    public boolean checkUserCredentials(String userId, String sessionId) {
        return false;
    }

    @Override
    public boolean userExists(String userId){
        QueryResult count = userCollection.count(new BasicDBObject("id", userId));
        long l = (Long) count.getResult().get(0);
        return l != 0;
    }


    @Override
    public QueryResult<User> createUser(User user) throws CatalogManagerException, JsonProcessingException {
        long startTime = startQuery();

        if(userExists(user.getId())){
            throw new CatalogManagerException("User {id:\""+user.getId()+"\"} already exists");
        }
        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        DBObject userDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
//        userDBObject.put("_id", user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogManagerException("User {id:\""+user.getId()+"\"} already exists");
        }
        String errorMsg = insert.getErrorMsg() != null? insert.getErrorMsg() : "";
        for (Project p : projects){
            String projectErrorMsg = createProject(user.getId(), p).getErrorMsg();
            if(projectErrorMsg != null && !projectErrorMsg.isEmpty()){
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

//        nativeUserCollection.findAndModify(new BasicDBObject("id", user.getId()),)

        //Get the inserted user.
        List<User> result = getUser(user.getId(), "").getResult();

        return endQuery("createUser", startTime, result, errorMsg, null);
    }

    @Override //TODO
    public QueryResult deleteUser(String userId) throws CatalogManagerException {
        long startTime = startQuery();
        //throw new UnsupportedOperationException();
        return null;
    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
        if(count.getResult().get(0) == 0){
            throw new CatalogManagerException("Bad user or password");
        } else {

            QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
            if (countSessions.getResult().get(0) != 0) {
                throw new CatalogManagerException("Already logged");
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
    public QueryResult logout(String userId, String sessionId) throws CatalogManagerException{
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
            throw new CatalogManagerException("UserId mismatches with the sessionId");
        }

        return endQuery("Logout", startTime);
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogManagerException {
        long startTime = startQuery();

        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if(countSessions.getResult().get(0) != 0){
            throw new CatalogManagerException("Error, sessionID already exists");
        }
        String userId = "anonymous_" + session.getId();
        User user = new User(userId, "Anonymous", "", "", "", User.ROLE_ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous = null;
        try {
            anonymous = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("Error parsing user to json", e);
        }
        anonymous.put("_id", user.getId());

        try {
            userCollection.insert(anonymous);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogManagerException("Anonymous user {id:\""+user.getId()+"\"} already exists");
        }

        ObjectMap resultObjectMap = new ObjectMap();
        resultObjectMap.put("sessionId", session.getId());
        resultObjectMap.put("userId", userId);
        return endQuery("Login as anonymous", startTime, Arrays.asList(resultObjectMap));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) throws CatalogManagerException {
        long startTime = startQuery();
        String userId = "anonymous_" + sessionId;
        logout(userId, sessionId);
        deleteUser(userId);
        return endQuery("Logout anonymous", startTime);
    }

    @Override
    public QueryResult<User> getUser(String userId, String lastActivityg) throws CatalogManagerException{
        long startTime = startQuery();
        //TODO: ManageSession
        //TODO: Check "lastActivity". If lastActivity == user.getLastActivity, return []
        DBObject query = new BasicDBObject("id", userId);
        QueryResult result = userCollection.find(query, null, null);

        User user = parseUser(result);
        if(user == null){
            throw new CatalogManagerException("User  {id:" + userId + "} not found");
        }
        return endQuery("Get user", startTime, Arrays.asList(user));
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("password", oldPassword);
        BasicDBObject fields = new BasicDBObject("password", newPassword);
        BasicDBObject action = new BasicDBObject("$set", fields);
        QueryResult<WriteResult> update = userCollection.update(query, action, false, false);
        if(update.getResult().get(0).getN() == 0){  //0 query matches.
            throw new CatalogManagerException("Bad user or password");
        }
        return endQuery("Change Password", startTime, update);
    }

    @Override
    public QueryResult changeEmail(String userId, String newEmail) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult modifyUser(String userId, Map<String, String> parameters) throws CatalogManagerException {
        long startTime = startQuery();

        if(!userExists(userId)){
            throw new CatalogManagerException("User {id:\""+userId+"\"} not found");
        }

        for (String s : parameters.keySet()) {
            if (!s.matches("name|email|organization|attributes\\..+|configs\\..+")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            }
        }

        userCollection.update(new BasicDBObject("id", userId), new BasicDBObject("$set", parameters), false, false);
        return endQuery("Modify user", startTime);
    }

    @Override
    public QueryResult resetPassword(String userId, String email) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult getSession(String userId, String sessionId) throws IOException {
        return null;
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

    public boolean projectExists(int projectId) {
        QueryResult count = userCollection.count(new BasicDBObject("projects.id", projectId));
        long l = (Long) count.getResult().get(0);
        return l != 0;
    }

    @Override
    public QueryResult<Project> createProject(String userId, Project project) throws CatalogManagerException, JsonProcessingException {
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
            throw new CatalogManagerException( "Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        //Generate json
        int projectId = getNewProjectId();
        project.setId(projectId);
        DBObject query = new BasicDBObject("id", userId);
        query.put("projects.alias", new BasicDBObject("$ne", project.getAlias()));
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", JSON.parse(jsonObjectWriter.writeValueAsString(project))));

        //Update object
        QueryResult<WriteResult> queryResult = userCollection.update(query, update, false, false);

        if (queryResult.getResult().get(0).getN() == 0) { // Check if the project has been inserted
            throw new CatalogManagerException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = createStudy(project.getId(), study).getErrorMsg();
            if(studyErrorMsg != null && !studyErrorMsg.isEmpty()){
                errorMsg += ", " + study.getAlias() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = getProject(project.getId()).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult<Project> getProject(String userId, String projectAlias) throws CatalogManagerException {
        int projectId = getProjectId(userId, projectAlias);
        return getProject(projectId);
    }

    @Override
    public QueryResult<Project> getProject(int projectId) throws CatalogManagerException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = new BasicDBObject(
                "projects",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("id", projectId)
                )
        );
        QueryResult result = userCollection.find(query, null, null, projection);
        User user = parseUser(result);
        if(user == null || user.getProjects().isEmpty()){
            throw new CatalogManagerException("Project {id:"+projectId+"} not found");
        }
        return endQuery("Get project", startTime, user.getProjects());
    }

    @Override
    public QueryResult deleteProject(int projecetId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult<Project> getAllProjects(String userId) throws CatalogManagerException {
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
    public QueryResult renameProjectAlias(int projectId, String newProjectAlias) throws CatalogManagerException {
        long startTime = startQuery();
//        String projectOwner = getProjectOwner(projectId);
//
//        int collisionProjectId = getProjectId(projectOwner, newProjectAlias);
//        if (collisionProjectId != -1) {
//            throw new CatalogManagerException("Couldn't rename project alias, alias already used in the same user");
//        }

        QueryResult<Project> projectResult = getProject(projectId); // if projectId doesn't exist, an exception is raised
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
            throw new CatalogManagerException("Project {alias:\"" + newProjectAlias+ "\"} already exists");
        }
        return endQuery("rename project alias", startTime, result);
    }

    @Override
    public QueryResult modifyProject(int projectId, Map<String, String> parameters) throws CatalogManagerException {
        long startTime = startQuery();

        //TODO: Check projectExists?
        BasicDBObject projectParameters = new BasicDBObject();
        for (String s : parameters.keySet()) {
            if (!s.matches("name|description|organization|status|attributes\\..+")) {
                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
            } else {
                projectParameters.put("projects.$."+s, parameters.get(s));
            }
        }

        userCollection.update(new BasicDBObject("projects.id", projectId), new BasicDBObject("$set", projectParameters), false, false);
        return endQuery("Modify user", startTime);
    }


    @Override
    public int getProjectId(String userId, String project) throws CatalogManagerException {
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", project)
                        .append("id", userId).get(),
                null,
                null,
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", project))).get()
        );
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            return -1;
        } else {
            return user.getProjects().get(0).getId();
        }
    }

    @Override
    public String getProjectOwner(int projectId) throws CatalogManagerException {
        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

        if(result.getResult().isEmpty()){
            throw new CatalogManagerException("Project {id:"+projectId+"} not found");
        } else {
            return result.getResult().get(0).get("id").toString();
        }
    }

    public Acl getFullProjectAcl(int projectId, String userId) throws CatalogManagerException {
        QueryResult<Project> project = getProject(projectId);
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
    public Acl getProjectAcl(int projectId, String userId) throws CatalogManagerException {
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
        if (aggregate.getNumResults() == 0) {
            return null;
        }

        Object aclObject = ((DBObject) ((DBObject) aggregate.getResult().get(0)).get("projects")).get("acl");
        Acl acl;
        try {
            acl = jsonObjectMapper.reader(Acl.class).readValue(aclObject.toString());
        } catch (IOException e) {
            throw new CatalogManagerException("get Project ACL: error parsing ACL");
        }
        return acl;
    }

    public void setProjectAcl(int projectId, Acl newAcl) throws CatalogManagerException {
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogManagerException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject;
        try {
            newAclObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(newAcl));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("could not put ACL: parsing error");
        }

        Acl projectAcl = getProjectAcl(projectId, userId);
        DBObject match;
        DBObject updateOperation;
        if (projectAcl == null) {  // there is no acl for that user in that project. push
            match = new BasicDBObject("projects.id", projectId);
            updateOperation = new BasicDBObject("$push", new BasicDBObject("projects.acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            match = BasicDBObjectBuilder
                    .start("id", projectId)
                    .append("acl.userId", userId).get();
            updateOperation = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        QueryResult update = fileCollection.update(match, updateOperation, false, false);
    }

    /**
     * Study methods
     * ***************************
     */
    @Override
    public QueryResult<Study> createStudy(int projectId, Study study) throws CatalogManagerException {
        long startTime = startQuery();
        if(projectId < 0){
            throw new CatalogManagerException("Project {id:"+projectId+"} not found");
        }
        //TODO: remove files and replace them with ids
        //TODO: Generate default folders.

        // Check if study.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start("projects.id", projectId)
                .append("projects.studies.alias", study.getAlias())
                .get();
        QueryResult<Long> queryResult = userCollection.count(countQuery);
        if (queryResult.getResult().get(0) != 0) {
            throw new CatalogManagerException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        study.setId(getNewStudyId());
        List<Analysis> analyses = study.getAnalyses();
        List<File> files = study.getFiles();
        study.setAnalyses(Collections.<Analysis>emptyList());
        study.setFiles(Collections.<File>emptyList());

        DBObject query = new BasicDBObject("projects.id", projectId);
        query.put("projects.studies.alias", new BasicDBObject("$ne", study.getAlias()));
        DBObject studyObject = null;
        try {
            studyObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(study));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("Error parsing study.", e);
        }
        DBObject update = new BasicDBObject("$push", new BasicDBObject(
                "projects.$.studies", studyObject ));
        QueryResult<WriteResult> updateResult = userCollection.update(query, update, false, false);

        //Check if the the study has been inserted
        if (updateResult.getResult().get(0).getN() == 0) {
            throw new CatalogManagerException("Study {alias:\"" + study.getAlias() + "\"} already exists");
        }

        String errorMsg = updateResult.getErrorMsg() != null? updateResult.getErrorMsg() : "";
        for(Analysis analysis : analyses){
            String analysisErrorMsg;
            analysisErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
            if(analysisErrorMsg != null && !analysisErrorMsg.isEmpty()){
                errorMsg += analysis.getAlias() + ":" + analysisErrorMsg + ", ";
            }
        }

        for (File file : files) {
            String fileErrorMsg;
            fileErrorMsg = createFileToStudy(study.getId(), file).getErrorMsg();
            if(fileErrorMsg != null && !fileErrorMsg.isEmpty()){
                errorMsg +=  file.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        List<Study> studyList = getStudy(study.getId()).getResult();
        return endQuery("Create Study", startTime, studyList, errorMsg, null);

    }

    @Override
    public QueryResult<Study> getAllStudies(int projectId) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("projects.id", projectId);
        DBObject projection = BasicDBObjectBuilder
                .start(new BasicDBObject(
                                "projects",
                                new BasicDBObject(
                                        "$elemMatch",
                                        new BasicDBObject("id", projectId)
                                )
                        )
                )
                .append("projects.studies", true).get();
        QueryResult queryResult = endQuery("get project", startTime
                , userCollection.find(query, null, null, projection));

        User user = parseUser(queryResult);
        if(user == null || user.getProjects().isEmpty()) {
            throw new CatalogManagerException("Project {id:"+projectId+"} not found");
        }
        List<Study> studies = user.getProjects().get(0).getStudies();
        for (Study study : studies) {
            study.setDiskUsage(getDiskUsageByStudy(study.getId()));
            study.setAnalyses(getAllAnalysis(study.getId()).getResult());
            //TODO: append files
        }
        return endQuery("Get all studies", startTime, studies);
    }


    @Override
    public QueryResult<Study> getStudy(int studyId) throws CatalogManagerException{
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
                )
                .append("projects.studies", true).get();

        QueryResult result = userCollection.find(query, null, null, projection);
        QueryResult queryResult = endQuery("get study", startTime, result);

        List<Study> studies;
        User user = parseUser(queryResult);
        if (user == null || user.getProjects().isEmpty()) {
            throw new CatalogManagerException("Study {id:"+studyId+"} not found");
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
                study.setAnalyses(getAllAnalysis(studyId).getResult());
            }
            // TODO study.setAnalysis
            // TODO study.setfiles
            studies = new LinkedList<>();
            studies.add(study);
        }
        //queryResult.setResult(studies);
        return endQuery("Get Study", startTime, studies);

    }

    private String getUserIdByStudyId(int studyId) {
        QueryResult id = userCollection.find(new BasicDBObject("projects.studies.id", studyId), null, null, new BasicDBObject("id", true));
        if(id.getNumResults() != 1){
            return null;
        } else {
            return (String) ((DBObject) id.getResult().get(0)).get("id");
        }
    }

    @Override
    public QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudyName) throws CatalogManagerException {
        throw new CatalogManagerException("Unsupported opperation");
    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyName) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        QueryResult studyResult = getStudy(studyId, sessionId);
        return null;
    }

    @Override
    public QueryResult deleteStudy(String userId, String projectAlias, String studyAlias) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult deleteStudy(int studyId) throws CatalogManagerException {
        return null;
    }

    @Override
    public int getStudyId(int projectId, String studyAlias) throws CatalogManagerException {
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
    public int getStudyId(String userId, String projectAlias, String studyAlias) throws CatalogManagerException {
        return getStudyId(getProjectId(userId, projectAlias), studyAlias);
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogManagerException {
        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject projection = new BasicDBObject("projects.id", "true");
        projection.put("projects", new BasicDBObject("$elemMatch", new BasicDBObject("studies.id", studyId)));
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

        User user = parseUser(result);
        if (user != null && !user.getProjects().isEmpty()) {
            return user.getProjects().get(0).getId();
        } else {
            throw new CatalogManagerException("Study {id:"+studyId+"} not found");
        }
    }

    @Override
    public String getStudyOwner(int studyId) throws CatalogManagerException {
        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> result = userCollection.find(query, null, null, projection);

        User user = parseUser(result);
        if (user != null) {
            return user.getId();
        } else {
            throw new CatalogManagerException("Study {id:"+studyId+"} not found");
        }
    }

    public Acl getStudyAcl(int studyId, String userId) throws CatalogManagerException {
        QueryResult<Study> studyQuery = getStudy(studyId);
        List<Acl> acl = studyQuery.getResult().get(0).getAcl();
        for (Acl acl1 : acl) {
            if (userId.equals(acl1.getUserId())) {
                return acl1;
            }
        }
        return null;
    }

    public void setStudyAcl(int studyId, Acl newAcl) throws CatalogManagerException {
        /*
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogManagerException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject;
        try {
            newAclObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(newAcl));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("could not put ACL: parsing error");
        }

        Acl studyAcl = getStudyAcl(studyId, userId);
        DBObject match;
        DBObject updateOperation;
        if (studyAcl == null) {  // there is no acl for that user in that file. push
            match = new BasicDBObject("id", studyId);
            updateOperation = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            match = BasicDBObjectBuilder
                    .start("id", studyId)
                    .append("acl.userId", userId).get();
            updateOperation = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        QueryResult update = fileCollection.update(match, updateOperation, false, false);
        */
    }
    /**
     * File methods
     * ***************************
     */

    @Override
    public QueryResult<File> createFileToStudy(String userId, String projectAlias, String studyAlias, File file) throws CatalogManagerException {
        long startTime = startQuery();

        int studyId = getStudyId(userId, projectAlias, studyAlias);
        if(studyId < 0){
            throw new CatalogManagerException("Study {alias:"+studyAlias+"} does not exists");
        }
        QueryResult<File> fileToStudy = createFileToStudy(studyId, file);
        return endQuery("Create file", startTime, fileToStudy);
    }

    @Override
    public QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogManagerException {
        long startTime = startQuery();

        String ownerId = getUserIdByStudyId(studyId);
        if(ownerId == null || ownerId.isEmpty()) {
            throw new CatalogManagerException("StudyID " + studyId + " not found");
        }
        BasicDBObject query = new BasicDBObject("studyId", studyId);
        query.put("uri", file.getUri());
        QueryResult<Long> count = fileCollection.count(query);
        if(count.getResult().get(0) != 0){
            throw new CatalogManagerException("File {studyId:"+ studyId +", uri:\""+file.getUri()+"\"} already exists");
        }


        int newFileId = getNewFileId();
        file.setId(newFileId);
        if(file.getCreatorId() == null) {
            file.setCreatorId(ownerId);
        }
        DBObject fileDBObject;
        try {
            fileDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(file));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("Error parsing file", e);
        }
        fileDBObject.put("studyId", studyId);
        try {
            fileCollection.insert(fileDBObject);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogManagerException("File {studyId:"+ studyId +", uri:\""+file.getUri()+"\"} already exists");
        }

        return endQuery("Create file", startTime, Arrays.asList(file));
    }

    @Override
    public QueryResult deleteFile(String userId, String projectAlias, String studyAlias, String uri) throws CatalogManagerException, IOException {
        return deleteFile(getFileId(userId, projectAlias, studyAlias, uri));
    }

    @Override
    public QueryResult deleteFile(int studyId, String uri) throws CatalogManagerException {
        return deleteFile(getFileId(studyId, uri));
    }

    @Override
    public QueryResult deleteFile(int fileId) throws CatalogManagerException {
        long startTime = startQuery();

        WriteResult id = nativeFileCollection.remove(new BasicDBObject("id", fileId));
        if(id.getN() == 0){
            throw new CatalogManagerException("file {id:"+fileId+"} not found");
        } else {
            return endQuery("Delete file", startTime, Arrays.asList(id.getN()));
        }
    }

    @Override
    public QueryResult deleteFilesFromStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult deleteFilesFromStudy(int studyId, String studyAlias, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public int getFileId(String userId, String projectAlias, String studyAlias, String uri) throws CatalogManagerException {
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        return getFileId(studyId, uri);
    }

    @Override
    public int getFileId(int studyId, String uri) throws CatalogManagerException {

        DBObject query = BasicDBObjectBuilder
                .start("studyId", studyId)
                .append("uri", uri).get();
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
    public QueryResult<File> getFile(String userId, String projectAlias, String studyAlias, String uri) throws CatalogManagerException {
        return getFile(getStudyId(userId, projectAlias, studyAlias), uri);
    }

    @Override
    public QueryResult<File> getFile(int studyId, String uri) throws CatalogManagerException {
        return getFile(getFileId(studyId, uri));
    }

    @Override
    public QueryResult<File> getFile(int fileId) throws CatalogManagerException {
        long startTime = startQuery();

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("id", fileId).get(),
                null,
                null,
                null
        );

        File file = parseFile(queryResult);
        if(file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw new CatalogManagerException("File {id:"+fileId+"} not found");
        }
    }

    @Override
    public QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, String uri, String status) throws CatalogManagerException, IOException {
        int fileId = getFileId(userId, projectAlias, studyAlias, uri);
        return setFileStatus(fileId, status);
    }

    @Override
    public QueryResult setFileStatus(int studyId, String uri, String status) throws CatalogManagerException, IOException {
        int fileId = getFileId(studyId, uri);
        return setFileStatus(fileId, status);
    }

    @Override
    public QueryResult setFileStatus(int fileId, String status) throws CatalogManagerException, IOException {
        long startTime = startQuery();
        System.out.println(fileId);
        BasicDBObject query = new BasicDBObject("id", fileId);
        BasicDBObject updates = new BasicDBObject("$set",
                new BasicDBObject("status", status));
        QueryResult<WriteResult> update = fileCollection.update(query, updates, false, false);
        if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
            throw new CatalogManagerException("File {id:"+fileId+"} not found");
        }
        return endQuery("Set file status", startTime);
    }

    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogManagerException {
        DBObject query = new BasicDBObject("id", fileId);
        DBObject projection = new BasicDBObject("studyId", "true");
        QueryResult<DBObject> result = fileCollection.find(query, null, null, projection);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get("studyId");
        } else {
            throw new CatalogManagerException("Study not found");
        }
    }

    @Override
    public String getFileOwner(int fileId) throws CatalogManagerException {
        int studyId = getStudyIdByFileId(fileId);
        return getStudyOwner(studyId);
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
    public Acl getFileAcl(int fileId, String userId) throws CatalogManagerException {
        DBObject projection = BasicDBObjectBuilder
                .start("acl",
                        new BasicDBObject("$elemMatch",
                                new BasicDBObject("userId", userId)))
                .append("_id", false)
                .get();

        QueryResult queryResult = fileCollection.find(new BasicDBObject("id", fileId), null, null, projection);
        if (queryResult.getNumResults() == 0) {
            throw new CatalogManagerException("getFileAcl: There is no file with fileId = " + fileId);
        }
        List<Acl> acl = parseFile(queryResult).getAcl();
        return acl == null ? null : acl.get(0);
    }


    public void setFileAcl(int fileId, Acl newAcl) throws CatalogManagerException {
        String userId = newAcl.getUserId();
        if (!userExists(userId)) {
            throw new CatalogManagerException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject;
        try {
            newAclObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(newAcl));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("could not put ACL: parsing error");
        }

        Acl fileAcl = getFileAcl(fileId, userId);
        DBObject match;
        DBObject updateOperation;
        if (fileAcl == null) {  // there is no acl for that user in that file. push
            match = new BasicDBObject("id", fileId);
            updateOperation = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            match = BasicDBObjectBuilder
                    .start("id", fileId)
                    .append("acl.userId", userId).get();
            updateOperation = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        QueryResult update = fileCollection.update(match, updateOperation, false, false);
    }

    /**
     * Analysis methods
     * ***************************
     */
    @Override
    public QueryResult<Analysis> getAllAnalysis(String userId, String projectAlias, String studyAlias) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        if (studyId < 0) {
            throw new CatalogManagerException("Study not found");
        } else {
            return getAllAnalysis(studyId);
        }
    }

    /**
     *  aggregation: db.user.aggregate([{"$match":{"analyses.studyId": 8}}, {$project:{analyses:1}}, {$unwind:"$analyses"}, {$match:{"analyses.studyId": 8}}, {$group:{"_id":"$studyId", analyses:{$push:"$analyses"}}}]).pretty()
     */
    @Override
    public QueryResult<Analysis> getAllAnalysis(int studyId) throws CatalogManagerException {
        long startTime = startQuery();

        DBObject match1 = new BasicDBObject("$match", new BasicDBObject("analyses.studyId", studyId));
        DBObject project = new BasicDBObject("$project", new BasicDBObject("analyses", 1));
        DBObject unwind = new BasicDBObject("$unwind", "$analyses");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("analyses.studyId", studyId));
        DBObject group = new BasicDBObject(
                "$group",
                BasicDBObjectBuilder
                        .start("_id", "$studyId")
                        .append("analyses", new BasicDBObject("$push", "$analyses")).get());

        List<DBObject> operations = new LinkedList<>();
        operations.add(match1);
        operations.add(project);
        operations.add(unwind);
        operations.add(match2);
        operations.add(group);
        QueryResult result = userCollection.aggregate(null, operations, null);

        List<Analysis> analyses = new LinkedList<>();
        try {
            if (result.getNumResults() != 0) {
                Study study = jsonStudyReader.readValue(result.getResult().get(0).toString());
                analyses = study.getAnalyses();
                // TODO fill analyses with jobs
            }
            return endQuery("get all analyses", startTime, analyses);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogManagerException("Failed to parse mongo : " + e.getMessage());
        }
    }

    /**
     * query: db.user.find({"analyses.alias":"analysis1Alias", "analyses.studyId":8}
     * , {"analyses":{$elemMatch:{studyId:8,alias:"analysis1Alias"}},"analyses.id":1}).pretty()
     */
    public int getAnalysisId(int studyId, String analysisAlias) throws CatalogManagerException {
        DBObject elem = new BasicDBObject("$elemMatch", BasicDBObjectBuilder
                .start("studyId", studyId)
                .append("alias", analysisAlias).get()
        );
        DBObject match = new BasicDBObject("analyses", elem);
        DBObject projection = BasicDBObjectBuilder
                .start("analyses", elem)
                .append("_id", false).get();
        QueryResult result = userCollection.find(match, null, null, projection);
        List<Analysis> analyses = parseAnalyses(result);
        return analyses.size() == 0? -1: analyses.get(0).getId();
    }

    @Override
    public QueryResult<Analysis> getAnalysis(int analysisId) throws CatalogManagerException {
        String queryId = "get analysis";
        long startTime = startQuery();
        DBObject query = new BasicDBObject("analyses.id", analysisId);
        DBObject projection = new BasicDBObject(
                "analyses",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("id", analysisId)
                )
        );

        QueryResult result = userCollection.find(query, null, null, projection);
        Study study;
        List<Analysis> analyses = new LinkedList<>();
        if (result.getNumResults() != 0) {
            try {
                study = jsonStudyReader.readValue(result.getResult().get(0).toString());
            } catch (IOException e) {
                throw new CatalogManagerException("Error parsing analysis", e);
            }
            analyses = study.getAnalyses();
        }
        return endQuery(queryId, startTime, analyses);
    }

    @Override
    public QueryResult createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis) throws CatalogManagerException {
        long startTime = startQuery();
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        if (studyId < 0) {
            throw new CatalogManagerException("Study not found");
        } else {
            return createAnalysis(studyId, analysis);
        }
    }

    @Override
    public QueryResult createAnalysis(int studyId, Analysis analysis) throws CatalogManagerException {
        long startTime = startQuery();
        // TODO manage session

        // Check if analysis.alias already exists.
        QueryResult<Long> count = userCollection.count(
                new BasicDBObject("analyses",
                        new BasicDBObject("$elemMatch", BasicDBObjectBuilder
                                .start("studyId", studyId)
                                .append("alias", analysis.getAlias()).get()
                        )
                )
        );
        if(count.getResult().get(0) != 0) {
            throw new CatalogManagerException("Analysis alias " + analysis.getAlias() + " already exists in study " + studyId);
        }

        // complete and push Analysis: id, studyId, jobs...
        analysis.setId(getNewAnalysisId());

        List<Job> jobs = analysis.getJobs();
        analysis.setJobs(Collections.<Job>emptyList());

        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject analysisObject;
        try {
            analysisObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(analysis));
        } catch (JsonProcessingException e) {
            throw new CatalogManagerException("analysis " + analysis.getAlias() + " could not be parsed into json", e);
        }
        analysisObject.put("studyId", studyId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject("analyses", analysisObject));
        QueryResult updateResult = userCollection.update(query, update, false, false);

        System.out.println(updateResult.getResult());
        // fill other collections: jobs
        if (jobs == null) {
            jobs = Collections.<Job>emptyList();
        }

        // TODO for (j:jobs) createJob(j)
//        for (Job job : jobs) {
//        }

        return endQuery("Create Analysis", startTime, getAnalysis(analysis.getId())); // seguir2 vuelve cuando hayas hecho el test de analysis

    }

    @Override
    public QueryResult<Job> createJob(int studyId, String analysisName, Job job, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }

    @Override
    public QueryResult deleteJob(int jobId, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public Job getJob(int jobId, String sessionId) throws CatalogManagerException, IOException {
        return null;
    }

    @Override
    public String getJobStatus(int jobId, String sessionId) throws CatalogManagerException, IOException {
        return null;
    }

    @Override
    public void incJobVisites(int jobId, String sessionId) throws CatalogManagerException, IOException {

    }

    @Override
    public void setJobCommandLine(int jobId, String commandLine, String sessionId) throws CatalogManagerException, IOException {

    }


    /*
    * Helper methods
    ********************/

    private User parseUser(QueryResult result) throws CatalogManagerException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonUserReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogManagerException("Error parsing user", e);
        }
    }

    private File parseFile(QueryResult result) throws CatalogManagerException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonFileReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogManagerException("Error parsing file", e);
        }
    }

    /**
     * must receive in the result: [{analyses:[{},...]}].
     * other keys in the object, besides "analyses", will throw
     * a parse error if they are not present in the Analyses Bean.
     */
    private List<Analysis> parseAnalyses(QueryResult result) throws CatalogManagerException {
        List<Analysis> analyses = new LinkedList<>();
        try {
            if (result.getNumResults() != 0) {
                Study study = jsonStudyReader.readValue(result.getResult().get(0).toString());
                analyses.addAll(study.getAnalyses());
                // TODO fill analyses with jobs
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogManagerException("Fail to parse analyses in mongo: " + e.getMessage());
        }
        return analyses;
    }

//    private User appendFilesToUser(User user, List<File> files) {
//
//    }
}
