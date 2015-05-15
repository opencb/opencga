/*
 * Copyright 2015 OpenCB
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
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.db.api.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor extends CatalogDBAdaptor
        implements CatalogUserDBAdaptor, CatalogStudyDBAdaptor, CatalogFileDBAdaptor, CatalogJobDBAdaptor, CatalogSampleDBAdaptor {

    private static final String USER_COLLECTION = "user";
    private static final String STUDY_COLLECTION = "study";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String METADATA_COLLECTION = "metadata";

    static final String METADATA_OBJECT_ID = "METADATA";

    //Keys to foreign objects.
    private static final String _ID = "_id";
    private static final String _PROJECT_ID = "_projectId";
    private static final String _STUDY_ID = "_studyId";
    private static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    private static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    private static final String FILTER_ROUTE_FILES =   "projects.studies.files.";
    private static final String FILTER_ROUTE_JOBS =    "projects.studies.jobs.";

    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration configuration;
    private final String database;
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
    private static Map<Class, ObjectReader> jsonReaderMap;

    @Override
    public CatalogUserDBAdaptor getCatalogUserDBAdaptor() {
        return this;
    }

    @Override
    public CatalogStudyDBAdaptor getCatalogStudyDBAdaptor() {
        return this;
    }

    @Override
    public CatalogFileDBAdaptor getCatalogFileDBAdaptor() {
        return this;
    }

    @Override
    public CatalogSampleDBAdaptor getCatalogSampleDBAdaptor() {
        return this;
    }

    @Override
    public CatalogJobDBAdaptor getCatalogJobDBAdaptor() {
        return this;
    }

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonReaderMap = new HashMap<>();
        jsonReaderMap.put(File.class, jsonFileReader = jsonObjectMapper.reader(File.class));
        jsonReaderMap.put(User.class, jsonUserReader = jsonObjectMapper.reader(User.class));
        jsonReaderMap.put(Job.class, jsonJobReader = jsonObjectMapper.reader(Job.class));
//        jsonProjectReader = jsonObjectMapper.reader(Project.class);
        jsonReaderMap.put(Study.class, jsonStudyReader = jsonObjectMapper.reader(Study.class));
//        jsonAnalysisReader = jsonObjectMapper.reader(Job.class);
        jsonReaderMap.put(Sample.class, jsonSampleReader = jsonObjectMapper.reader(Sample.class));
    }

    public CatalogMongoDBAdaptor(List<DataStoreServerAddress> dataStoreServerAddressList, MongoDBConfiguration configuration, String database)
            throws CatalogDBException {
        super();
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddressList);
        this.configuration = configuration;
        this.database = database;

        logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);

        connect();
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(database, configuration);
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
                DBObject metadataObject = getDbObject(new Metadata(), "Metadata");
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject, null);
                insertUser(new User("admin", "admin", "admin@email.com", "admin", "opencb", User.Role.ADMIN, "active"), new QueryOptions());

            } catch (DuplicateKeyException e){
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
            //Set indexes
//            BasicDBObject unique = new BasicDBObject("unique", true);
//            nativeUserCollection.createIndex(new BasicDBObject("id", 1), unique);
//            nativeFileCollection.createIndex(BasicDBObjectBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndex(new BasicDBObject("id", 1), unique);
        }
    }

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
                                        String organization, QueryOptions options) throws CatalogDBException {
        checkParameter(userId, "userId");
        long startTime = startQuery();

        if(userExists(userId)) {
            throw new CatalogDBException("User {id:\"" + userId + "\"} already exists");
        }
        return null;

    }

    @Override
    public QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException {
        checkParameter(user, "user");
        long startTime = startQuery();

        if(userExists(user.getId())) {
            throw new CatalogDBException("User {id:\"" + user.getId() + "\"} already exists");
        }

        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        user.setLastActivity(TimeUtils.getTimeMillis());
        DBObject userDBObject = getDbObject(user, "User " + user.getId());
        userDBObject.put("_id", user.getId());

        QueryResult insert;
        try {
            insert = userCollection.insert(userDBObject, null);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("User {id:\""+user.getId()+"\"} already exists");
        }

        String errorMsg = insert.getErrorMsg() != null ? insert.getErrorMsg() : "";
        for (Project p : projects) {
            String projectErrorMsg = createProject(user.getId(), p, options).getErrorMsg();
            if(projectErrorMsg != null && !projectErrorMsg.isEmpty()){
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

        //Get the inserted user.
        user.setProjects(projects);
        List<User> result = getUser(user.getId(), options, "").getResult();

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
        WriteResult wr = userCollection.remove(new BasicDBObject("id", userId), null).getResult().get(0);
        if (wr.getN() == 0) {
            throw CatalogDBException.idNotFound("User", userId);
        } else {
            return endQuery("Delete user", startTime, Arrays.asList(wr.getN()));
        }
    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException {
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
                        "sessions", getDbObject(session, "Sesion")
                )
                );
                userCollection.update(id, updates, null);

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
                    null);

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
        DBObject anonymous = getDbObject(user, "User");
        anonymous.put("_id", user.getId());

        try {
            userCollection.insert(anonymous, null);
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
        if (!userExists(userId)) {
            throw CatalogDBException.idNotFound("User", userId);
        }
        DBObject query = new BasicDBObject("id", userId);
        query.put("lastActivity", new BasicDBObject("$ne", lastActivity));
        QueryResult<DBObject> result = userCollection.find(query, options);
        User user = parseUser(result);
        if(user == null) {
            return endQuery("Get user", startTime); // user exists but no different lastActivity was found: return empty result
        } else {
            joinFields(user, options);
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
        QueryResult<WriteResult> update = userCollection.update(query, action, null);
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
    public QueryResult<User> modifyUser(String userId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> userParameters = new HashMap<>();

        String[] acceptedParams = {"name", "email", "organization", "lastActivity", "role", "status"};
        filterStringParams(parameters, userParameters, acceptedParams);
        String[] acceptedIntParams = {"diskQuota", "diskUsage"};
        filterIntParams(parameters, userParameters, acceptedIntParams);

        String[] acceptedMapParams = {"attributes", "configs"};
        filterMapParams(parameters, userParameters, acceptedMapParams);

        if(!userParameters.isEmpty()) {
            QueryResult<WriteResult> update = userCollection.update(
                    new BasicDBObject("id", userId),
                    new BasicDBObject("$set", userParameters), null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
                throw CatalogDBException.idNotFound("User", userId);
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
        QueryResult<WriteResult> update = userCollection.update(query, action, null);
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
                new BasicDBObject("id", true),
                null);

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
    public QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException {
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
        DBObject projectDBObject = getDbObject(project, "Project");
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", projectDBObject));

        //Update object
        QueryResult<WriteResult> queryResult = userCollection.update(query, update, null);

        if (queryResult.getResult().get(0).getN() == 0) { // Check if the project has been inserted
            throw new CatalogDBException("Project {alias:\"" + project.getAlias() + "\"} already exists in this user");
        }

        String errorMsg = "";
        for (Study study : studies) {
            String studyErrorMsg = createStudy(project.getId(), study, options).getErrorMsg();
            if(studyErrorMsg != null && !studyErrorMsg.isEmpty()){
                errorMsg += ", " + study.getAlias() + ":" + studyErrorMsg;
            }
        }
        List<Project> result = getProject(project.getId(), null).getResult();
        return endQuery("Create Project", startTime, result, errorMsg, null);
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
        QueryResult<DBObject> result = userCollection.find(query, projection, options);
        User user = parseUser(result);
        if(user == null || user.getProjects().isEmpty()) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }
        List<Project> projects = user.getProjects();
        joinFields(projects.get(0), options);

        return endQuery("Get project", startTime, projects);
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

        QueryResult<WriteResult> update = userCollection.update(query, pull, null);
        List<Integer> deletes = new LinkedList<>();
        if (update.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("Project", projectId);
        } else {
            deletes.add(update.getResult().get(0).getN());
            return endQuery("delete project", startTime, deletes);
        }
    }

    @Override
    public QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject("projects", true);
        projection.put("_id", false);
        QueryResult<DBObject> result = userCollection.find(query, projection, options);

        User user = parseUser(result);
        List<Project> projects = user.getProjects();
        for (Project project : projects) {
            joinFields(project, options);
        }
        return endQuery(
                "User projects list", startTime,
                projects);
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

        QueryResult<WriteResult> result = userCollection.update(query, update, null);
        if (result.getResult().get(0).getN() == 0) {    //Check if the the study has been inserted
            throw new CatalogDBException("Project {alias:\"" + newProjectAlias+ "\"} already exists");
        }
        return endQuery("rename project alias", startTime, result);
    }

    @Override
    public QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        if (!projectExists(projectId)) {
            throw CatalogDBException.idNotFound("Project", projectId);
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
            QueryResult<WriteResult> updateResult = userCollection.update(query, updates, null);
            if(updateResult.getResult().get(0).getN() == 0){
                throw CatalogDBException.idNotFound("Project", projectId);
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
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", projectAlias))).get(),
                null
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
        QueryResult<DBObject> result = userCollection.find(query, projection, null);

        if(result.getResult().isEmpty()){
            throw CatalogDBException.idNotFound("Project", projectId);
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
        QueryResult aggregate = userCollection.aggregate(operations, null);

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

        DBObject newAclObject = getDbObject(newAcl, "ACL");

        List<Acl> projectAcls = getProjectAcl(projectId, userId).getResult();
        DBObject query = new BasicDBObject("projects.id", projectId);
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("projects.$.acl", newAclObject));
        if (!projectAcls.isEmpty()) {  // ensure that there is no acl for that user in that project. pull
            DBObject pull = new BasicDBObject("$pull", new BasicDBObject("projects.$.acl", new BasicDBObject("userId", userId)));
            userCollection.update(query, pull, null);
        }
        //Put study
        QueryResult pushResult = userCollection.update(query, push, null);
        return endQuery("Set project acl", startTime, pushResult);
    }


//    public QueryResult<Project> searchProject(QueryOptions query, QueryOptions options) throws CatalogDBException {
//        long startTime = startQuery();
//
//
//        return endQuery("Search Proyect", startTime, projects);
//    }

    /**
     * Study methods
     * ***************************
     */

    @Override
    public boolean studyExists(int studyId) {
        QueryResult<Long> count = studyCollection.count(new BasicDBObject("id", studyId));
        return count.getResult().get(0) != 0;
    }

    private void checkStudyId(int studyId) throws CatalogDBException {
        if(!studyExists(studyId)) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
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
    public QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if(projectId < 0){
            throw CatalogDBException.idNotFound("Project", projectId);
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
        DBObject studyObject = getDbObject(study, "Study");
        studyObject.put(_ID, newId);

        //Set ProjectId
        studyObject.put(_PROJECT_ID, projectId);

        //Insert
        QueryResult<WriteResult> updateResult = studyCollection.insert(studyObject, null);

        //Check if the the study has been inserted
//        if (updateResult.getResult().get(0).getN() == 0) {
//            throw new CatalogDBException("Study {alias:\"" + study.getAlias() + "\"} already exists");
//        }

        // Insert nested fields
        String errorMsg = updateResult.getErrorMsg() != null? updateResult.getErrorMsg() : "";

        for (File file : files) {
            String fileErrorMsg = createFileToStudy(study.getId(), file, options).getErrorMsg();
            if(fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
                errorMsg +=  file.getName() + ":" + fileErrorMsg + ", ";
            }
        }

        for (Job job : jobs) {
//            String jobErrorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
            String jobErrorMsg = createJob(study.getId(), job, options).getErrorMsg();
            if(jobErrorMsg != null && !jobErrorMsg.isEmpty()){
                errorMsg += job.getName() + ":" + jobErrorMsg + ", ";
            }
        }

        List<Study> studyList = getStudy(study.getId(), options).getResult();
        return endQuery("Create Study", startTime, studyList, errorMsg, null);

    }

    @Override
    public QueryResult<Study> getAllStudies(int projectId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if(!projectExists(projectId)) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }

        DBObject query = new BasicDBObject(_PROJECT_ID, projectId);

        QueryResult<DBObject> queryResult = studyCollection.find(query, filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Study> studies = parseStudies(queryResult);
        for (Study study : studies) {
            joinFields(study, options);
        }
        return endQuery("Get all studies", startTime, studies);
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        //TODO: Parse QueryOptions include/exclude
        DBObject query = new BasicDBObject("id", studyId);
        QueryResult result = studyCollection.find(query, filterOptions(options, FILTER_ROUTE_STUDIES));
//        QueryResult queryResult = endQuery("get study", startTime, result);

        List<Study> studies = parseStudies(result);
        if (studies.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        joinFields(studies.get(0), options);

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

        checkStudyId(studyId);
        BasicDBObject studyParameters = new BasicDBObject();

        String[] acceptedParams = {"name", "creationDate", "creationId", "description", "status", "lastActivity", "cipher"};
        filterStringParams(parameters, studyParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, parameters, acceptedLongParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, studyParameters, acceptedMapParams);

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
            QueryResult<WriteResult> updateResult = studyCollection.update(query, updates, null);
            if(updateResult.getResult().get(0).getN() == 0){
                throw CatalogDBException.idNotFound("Study", studyId);
            }
        }
        return endQuery("Modify study", startTime, Collections.singletonList(new ObjectMap(studyParameters)));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException {
        long startTime = startQuery();
        DBObject query = new BasicDBObject("id", studyId);
        QueryResult<WriteResult> remove = studyCollection.remove(query, null);

        List<Integer> deletes = new LinkedList<>();

        if (remove.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        } else {
            deletes.add(remove.getResult().get(0).getN());
            return endQuery("delete study", startTime, deletes);
        }
    }

    @Override
    public int getStudyId(int projectId, String studyAlias) throws CatalogDBException {
        DBObject query = BasicDBObjectBuilder.start(_PROJECT_ID, projectId).append("alias", studyAlias).get();
        BasicDBObject projection = new BasicDBObject("id", "true");
        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, null);
        List<Study> studies = parseStudies(queryResult);
        return studies == null || studies.isEmpty() ? -1 : studies.get(0).getId();
    }

    @Override
    public int getProjectIdByStudyId(int studyId) throws CatalogDBException {
        DBObject query = new BasicDBObject("id", studyId);
        DBObject projection = new BasicDBObject(_PROJECT_ID, "true");
        QueryResult<DBObject> result = studyCollection.find(query, projection, null);

        if (!result.getResult().isEmpty()) {
            DBObject study = result.getResult().get(0);
            return Integer.parseInt(study.get(_PROJECT_ID).toString());
        } else {
            throw CatalogDBException.idNotFound("Study", studyId);
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
        QueryResult<DBObject> dbObjectQueryResult = studyCollection.find(query, projection, null);
        List<Study> studies = parseStudies(dbObjectQueryResult);
        if(studies.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
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

        DBObject newAclObject = getDbObject(newAcl, "ACL");

        BasicDBObject query = new BasicDBObject("id", studyId);
        BasicDBObject pull = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", newAcl.getUserId())));
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        studyCollection.update(query, pull, null);
        studyCollection.update(query, push, null);

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
    public QueryResult<File> createFileToStudy(int studyId, File file, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        String ownerId = getStudyOwnerId(studyId);
        if(ownerId == null || ownerId.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
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
        DBObject fileDBObject = getDbObject(file, "File");
        fileDBObject.put(_STUDY_ID, studyId);
        fileDBObject.put(_ID, newFileId);

        try {
            fileCollection.insert(fileDBObject, null);
        } catch (MongoException.DuplicateKey e) {
            throw new CatalogDBException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
        }

        return endQuery("Create file", startTime, getFile(newFileId, options));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException {
        long startTime = startQuery();

        WriteResult id = fileCollection.remove(new BasicDBObject("id", fileId), null).getResult().get(0);
        List<Integer> deletes = new LinkedList<>();
        if(id.getN() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
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
        BasicDBObject projection = new BasicDBObject("id", true);
        QueryResult<DBObject> queryResult = fileCollection.find(query, projection, null);
        File file = parseFile(queryResult);
        return file != null ? file.getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFiles(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> queryResult = fileCollection.find( new BasicDBObject(_STUDY_ID, studyId), filterOptions(options, FILTER_ROUTE_FILES));
        List<File> files = parseFiles(queryResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> folderResult = fileCollection.find( new BasicDBObject("id", folderId), filterOptions(options, FILTER_ROUTE_FILES));

        File folder = parseFile(folderResult);
        if (!folder.getType().equals(File.Type.FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Object studyId = folderResult.getResult().get(0).get(_STUDY_ID);

        BasicDBObject query = new BasicDBObject(_STUDY_ID, studyId);
        query.put("path", new BasicDBObject("$regex", "^" + folder.getPath() + "[^/]+/?$"));
        QueryResult<DBObject> filesResult = fileCollection.find(query, null);
        List<File> files = parseFiles(filesResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryOptions filterOptions = filterOptions(options, FILTER_ROUTE_FILES);

        QueryResult<DBObject> queryResult = fileCollection.find( new BasicDBObject("id", fileId), filterOptions);

        File file = parseFile(queryResult);
        if(file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public QueryResult setFileStatus(int fileId, File.Status status) throws CatalogDBException {
        long startTime = startQuery();
        return endQuery("Set file status", startTime, modifyFile(fileId, new ObjectMap("status", status.toString())));
    }

    @Override
    public QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {"type", "format", "bioformat", "uriScheme", "description", "status", "uri"};
        filterStringParams(parameters, fileParameters, acceptedParams);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, fileParameters, acceptedLongParams);

        String[] acceptedIntParams = {"jobId"};
        filterIntParams(parameters, fileParameters, acceptedIntParams);

        String[] acceptedIntegerListParams = {"sampleIds"};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, fileParameters, acceptedMapParams);

        String[] acceptedObjectParams = {"index"};
        filterObjectParams(parameters, fileParameters, acceptedObjectParams);

        if(!fileParameters.isEmpty()) {
            QueryResult<WriteResult> update = fileCollection.update(new BasicDBObject("id", fileId),
                    new BasicDBObject("$set", fileParameters), null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
                throw CatalogDBException.idNotFound("File", fileId);
            }
        }

        return endQuery("Modify file", startTime);
    }


    /**
     * @param filePath assuming 'pathRelativeToStudy + name'
     */
    @Override
    public QueryResult renameFile(int fileId, String filePath) throws CatalogDBException {
        long startTime = startQuery();

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        File file = getFile(fileId, null).getResult().get(0);

        int studyId = getStudyIdByFileId(fileId);
        int collisionFileId = getFileId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.FOLDER)) {  // recursive over the files inside folder
            QueryResult<File> allFilesInFolder = getAllFilesInFolder(fileId, null);
            String oldPath = file.getPath();
            filePath += filePath.endsWith("/")? "" : "/";
            for (File subFile : allFilesInFolder.getResult()) {
                String replacedPath = subFile.getPath().replace(oldPath, filePath);
                renameFile(subFile.getId(), replacedPath); // first part of the path in the subfiles 3
            }
        }
        BasicDBObject query = new BasicDBObject("id", fileId);
        BasicDBObject set = new BasicDBObject("$set", BasicDBObjectBuilder
                .start("name", fileName)
                .append("path", filePath).get());
        QueryResult<WriteResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return endQuery("rename file", startTime);
    }


    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
        DBObject query = new BasicDBObject("id", fileId);
        DBObject projection = new BasicDBObject(_STUDY_ID, "true");
        QueryResult<DBObject> result = fileCollection.find(query, projection, null);

        if (!result.getResult().isEmpty()) {
            return (int) result.getResult().get(0).get(_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public String getFileOwnerId(int fileId) throws CatalogDBException {
        QueryResult<File> fileQueryResult = getFile(fileId, null);
        if(fileQueryResult == null || fileQueryResult.getResult() == null || fileQueryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return fileQueryResult.getResult().get(0).getOwnerId();
//        int studyId = getStudyIdByFileId(fileId);
//        return getStudyOwnerId(studyId);
    }

    private long getDiskUsageByStudy(int studyId){
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
                                .start("_id", "$" + _STUDY_ID)
                                .append("diskUsage",
                                        new BasicDBObject(
                                                "$sum",
                                                "$diskUsage"
                                        )).get()
                )
        );
        QueryResult<DBObject> aggregate = fileCollection.aggregate(operations, null);
        if(aggregate.getNumResults() == 1){
            Object diskUsage = aggregate.getResult().get(0).get("diskUsage");
            if(diskUsage instanceof Integer){
                return ((Integer) diskUsage).longValue();
            } else if (diskUsage instanceof Long) {
                return ((Long) diskUsage);
            } else {
                return Long.parseLong(diskUsage.toString());
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

        QueryResult queryResult = fileCollection.find(new BasicDBObject("id", fileId), projection, null);
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

        DBObject newAclObject = getDbObject(newAcl, "ACL");

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
        QueryResult update = fileCollection.update(match, updateOperation, null);
        return endQuery("set file acl", startTime);
    }

    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBList filters = new BasicDBList();
        DBObject mongoQuery = new BasicDBObject();

        if (query.containsKey(FileFilterOption.id.name())) {
            addQueryIntegerListFilter(FileFilterOption.id.name(), query, "_id", mongoQuery);
        }
        if (query.containsKey(FileFilterOption.studyId.name())) {
            addQueryIntegerListFilter(FileFilterOption.studyId.name(), query, _STUDY_ID, mongoQuery);
        }
        if (query.containsKey(FileFilterOption.name.name())) {
            addQueryStringListFilter(FileFilterOption.name.name(), query, mongoQuery);
        }
        if (query.containsKey(FileFilterOption.type.name())) {
            addQueryStringListFilter(FileFilterOption.type.name(), query, mongoQuery);
        }
        if (query.containsKey(FileFilterOption.path.name())) {
            addQueryStringListFilter(FileFilterOption.path.name(), query, mongoQuery);
        }
        if (query.containsKey(FileFilterOption.bioformat.name())) {
            addQueryStringListFilter(FileFilterOption.bioformat.name(), query, mongoQuery);
        }
        if (query.containsKey(FileFilterOption.status.name())) {
            addQueryStringListFilter("status", query, mongoQuery);
        }
        if (query.containsKey(FileFilterOption.maxSize.name())) {
            mongoQuery.put("size", new BasicDBObject("$lt", query.getInt("maxSize")));
        }
        if (query.containsKey(FileFilterOption.minSize.name())) {
            mongoQuery.put("size", new BasicDBObject("$gt", query.getInt("minSize")));
        }
        if (query.containsKey(FileFilterOption.startDate.name())) {
            mongoQuery.put("creationDate", new BasicDBObject("$lt", query.getString("startDate")));
        }
        if (query.containsKey(FileFilterOption.endDate.name())) {
            mongoQuery.put("creationDate", new BasicDBObject("$gt", query.getString("endDate")));
        }
        if (query.containsKey(FileFilterOption.like.name())) {
            mongoQuery.put("name", new BasicDBObject("$regex", query.getString("like")));
        }
        if (query.containsKey(FileFilterOption.startsWith.name())) {
            mongoQuery.put("name", new BasicDBObject("$regex", "^" + query.getString("startsWith")));
        }
        if (query.containsKey(FileFilterOption.directory.name())) {
            mongoQuery.put("path", new BasicDBObject("$regex", "^" + query.getString("directory") + "[^/]+/?$"));
        }
        if (query.containsKey(FileFilterOption.attributes.name())) {
            addCompQueryFilter(FileFilterOption.attributes.name(), query, "attributes", mongoQuery);
        }

//        DBObject query = new BasicDBObject("$and", filters);
//        QueryResult<DBObject> queryResult = fileCollection.find(query, null);

        QueryResult<DBObject> queryResult = fileCollection.find(mongoQuery, filterOptions(options, FILTER_ROUTE_FILES));

        List<File> files = parseFiles(queryResult);

        return endQuery("Search File", startTime, files);
    }

    @Override
    public QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        checkStudyId(studyId);

        QueryResult<Long> count = studyCollection.count(BasicDBObjectBuilder
                .start(_ID, studyId)
                .append("datasets.name", dataset.getName())
                .get());

        if(count.getResult().get(0) > 0) {
            throw new CatalogDBException("Dataset { name: \"" + dataset.getName() + "\" } already exists in this study.");
        }

        int newId = getNewId();
        dataset.setId(newId);

        DBObject datasetObject = getDbObject(dataset, "Dataset");
        QueryResult<WriteResult> update = studyCollection.update(
                new BasicDBObject(_ID, studyId),
                new BasicDBObject("$push", new BasicDBObject("datasets", datasetObject)), null);

        if (update.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        return endQuery("createDataset", startTime, getDataset(newId, options));
    }

    @Override
    public QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("datasets.id", datasetId);
        BasicDBObject projection = new BasicDBObject("datasets", new BasicDBObject("$elemMatch", new BasicDBObject("id", datasetId)));
        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Study> studies = parseStudies(queryResult);
        if(studies == null || studies.get(0).getDatasets().isEmpty()) {
            throw CatalogDBException.idNotFound("Dataset", datasetId);
        } else {
            return endQuery("readDataset", startTime, studies.get(0).getDatasets());
        }
    }

    @Override
    public int getStudyIdByDatasetId(int datasetId) throws CatalogDBException {
        BasicDBObject query = new BasicDBObject("datasets.id", datasetId);
        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", 1), null);
        if(queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsField("id")) {
            throw CatalogDBException.idNotFound("Dataset", datasetId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Integer ? (Integer) id : Integer.parseInt(id.toString());
        }
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
    public QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        checkStudyId(studyId);

        int jobId = getNewId();
        job.setId(jobId);

        DBObject jobObject = getDbObject(job, "job");
        jobObject.put(_ID, jobId);
        jobObject.put(_STUDY_ID, studyId);
        QueryResult insertResult = jobCollection.insert(jobObject, null); //TODO: Check results.get(0).getN() != 0

        return endQuery("Create Job", startTime, getJob(jobId, filterOptions(options, FILTER_ROUTE_JOBS)));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Job> deleteJob(int jobId) throws CatalogDBException {
        long startTime = startQuery();
        Job job = getJob(jobId, null).first();
        WriteResult id = jobCollection.remove(new BasicDBObject("id", jobId), null).getResult().get(0);
        List<Integer> deletes = new LinkedList<>();
        if (id.getN() == 0) {
            throw CatalogDBException.idNotFound("Job", jobId);
        } else {
            deletes.add(id.getN());
            return endQuery("delete job", startTime, Collections.singletonList(job));
        }
    }

    @Override
    public QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<DBObject> queryResult = jobCollection.find(new BasicDBObject("id", jobId), filterOptions(options, FILTER_ROUTE_JOBS));
        Job job = parseJob(queryResult);
        if(job != null) {
            return endQuery("Get job", startTime, Arrays.asList(job));
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> getAllJobs(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<DBObject> queryResult = jobCollection.find(new BasicDBObject(_STUDY_ID, studyId), filterOptions(options, FILTER_ROUTE_JOBS));
        List<Job> jobs = parseJobs(queryResult);
        return endQuery("Get all jobs", startTime, jobs);
    }

    @Override
    public String getJobStatus(int jobId, String sessionId) throws CatalogDBException {   // TODO remove?
        throw new UnsupportedOperationException("Not implemented method");
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("id", jobId);
        Job job = parseJob(jobCollection.<DBObject>find(query, new BasicDBObject("visits", true), null));
        int visits;
        if (job != null) {
            visits = job.getVisits()+1;
            BasicDBObject set = new BasicDBObject("$set", new BasicDBObject("visits", visits));
            jobCollection.update(query, set, null);
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
        return endQuery("Inc visits", startTime, Arrays.asList(new ObjectMap("visits", visits)));
    }

    @Override
    public QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> jobParameters = new HashMap<>();

        String[] acceptedParams = {"name", "userId", "toolName", "date", "description", "outputError", "commandLine", "status", "outdir", "error", "errorDescription"};
        filterStringParams(parameters, jobParameters, acceptedParams);

        String[] acceptedIntParams = {"visits"};
        filterIntParams(parameters, jobParameters, acceptedIntParams);

        String[] acceptedLongParams = {"startTime", "endTime", "diskUsage"};
        filterLongParams(parameters, jobParameters, acceptedLongParams);

        String[] acceptedIntegerListParams = {"output"};
        filterIntegerListParams(parameters, jobParameters, acceptedIntegerListParams);

        String[] acceptedMapParams = {"attributes", "resourceManagerAttributes"};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        if(!jobParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject("id", jobId);
            BasicDBObject updates = new BasicDBObject("$set", jobParameters);
//            System.out.println("query = " + query);
//            System.out.println("updates = " + updates);
            QueryResult<WriteResult> update = jobCollection.update(query, updates, null);
            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
                throw CatalogDBException.idNotFound("Job", jobId);
            }
        }
        return endQuery("Modify job", startTime);
    }

    @Override
    public int getStudyIdByJobId(int jobId) throws CatalogDBException {
        DBObject query = new BasicDBObject("id", jobId);
        DBObject projection = new BasicDBObject(_STUDY_ID, true);
        QueryResult<DBObject> id = jobCollection.find(query, projection, null);

        if (id.getNumResults() != 0) {
            return Integer.parseInt(id.getResult().get(0).get(_STUDY_ID).toString());
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> searchJob(QueryOptions query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        DBObject mongoQuery = new BasicDBObject();

        if(query.containsKey("ready")) {
            if(query.getBoolean("ready")) {
                mongoQuery.put("status", Job.Status.READY.name());
            } else {
                mongoQuery.put("status", new BasicDBObject("$ne", Job.Status.READY.name()));
            }
            query.remove("ready");
        }

        if (query.containsKey("studyId")) {
            addQueryIntegerListFilter("studyId", query, _STUDY_ID, mongoQuery);
        }

        if (query.containsKey("status")) {
            addQueryStringListFilter("status", query, mongoQuery);
        }

//        System.out.println("query = " + query);
        QueryResult<DBObject> queryResult = jobCollection.find(mongoQuery, null);
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

        DBObject toolObject = getDbObject(tool, "tool");
        DBObject query = new BasicDBObject("id", userId);
        query.put("tools.alias", new BasicDBObject("$ne", tool.getAlias()));
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("tools", toolObject));

        //Update object
        QueryResult<WriteResult> queryResult = userCollection.update(query, update, null);

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
        QueryResult<DBObject> queryResult = userCollection.find(query, projection, new QueryOptions("include", Collections.singletonList("tools")));

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

        QueryResult<DBObject> queryResult = userCollection.find(query, projection, null);
        if(queryResult.getNumResults() != 1 ) {
            throw new CatalogDBException("Tool {alias:" + toolAlias + "} no exists");
        }
        User user = parseUser(queryResult);
        return user.getTools().get(0).getId();
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

    /**
     * Experiments methods
     * ***************************
     */

    @Override
    public boolean experimentExists(int experimentId) {
        return false;
    }

    /**
     * Samples methods
     * ***************************
     */

    @Override
    public boolean sampleExists(int sampleId) {
        DBObject query = new BasicDBObject("id", sampleId);
        QueryResult<Long> count = sampleCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    @Override
    public QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        checkStudyId(studyId);
        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("name", sample.getName()).append(_STUDY_ID, studyId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        int sampleId = getNewId();
        sample.setId(sampleId);
        sample.setAnnotationSets(Collections.<AnnotationSet>emptyList());
        //TODO: Add annotationSets
        DBObject sampleObject = getDbObject(sample, "sample");
        sampleObject.put(_STUDY_ID, studyId);
        sampleObject.put(_ID, sampleId);
        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, getSample(sampleId, options));
    }


    @Override
    public QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
        DBObject query = new BasicDBObject("id", sampleId);

        QueryResult<DBObject> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        if(samples.isEmpty()) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }

        return endQuery("getSample", startTime, samples);
    }

    @Override
    public QueryResult<Sample> getAllSamples(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        String warning = "";

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
        DBObject query = new BasicDBObject(_STUDY_ID, studyId);

        // Sample Filters  //
        addQueryIntegerListFilter("id", options, "_id", query);
        addQueryStringListFilter("name", options, query);
        addQueryStringListFilter("source", options, query);

        // AnnotationSet Filters //
        BasicDBObject annotationSetFilter = new BasicDBObject();
        addQueryIntegerListFilter("variableSetId", options, annotationSetFilter);
        addQueryStringListFilter("annotationSetId", options, "id", annotationSetFilter);


        List<DBObject> annotationFilters = new LinkedList<>();
        // Annotation Filters
        if (options.containsKey("annotation")) {
            List<String> annotations = options.getAsStringList("annotation");
            for (String annotation : annotations) {
                String[] split = annotation.split(":", 2);
                if (split.length != 2) {
                    String w = "Malformed annotation query : " + annotation;
                    warning += w + "\n";
                    logger.warn(warning);
                    continue;
                }
//                annotationFilters.add(
//                        new BasicDBObject("annotations",
//                                new BasicDBObject("$elemMatch", BasicDBObjectBuilder
//                                        .start("id", split[0])
//                                        .add("value", split[1]).get()
//                                )
//                        )
//                );
                String[] values = split[1].split(",");
                if (values.length > 1) {
                    annotationFilters.add(new BasicDBObject("_annotMap" + "." + split[0], new BasicDBObject("$in", Arrays.asList(values)) ));
                } else {
                    annotationFilters.add(new BasicDBObject("_annotMap" + "." + split[0], split[1] ));
                }
            }
        }

        if (!annotationFilters.isEmpty()) {
            annotationSetFilter.put("$and", annotationFilters);
        }
        if (!annotationSetFilter.isEmpty()) {
            query.put("annotationSets", new BasicDBObject("$elemMatch", annotationSetFilter));
        }
        QueryResult<DBObject> queryResult = sampleCollection.find(query, filteredOptions);
        List<Sample> samples = parseSamples(queryResult);

        QueryResult<Sample> result = endQuery("getAllSamples", startTime, samples, null, warning.isEmpty() ? null : warning);
        result.setNumTotalResults(queryResult.getNumTotalResults());
        return result;
    }

    @Override
    public QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException {
        //TODO
        throw new UnsupportedOperationException("No implemented");
    }

    @Override
    public QueryResult<Integer> deleteSample(int sampleId) throws CatalogDBException {
        long startTime = startQuery();

        WriteResult id = sampleCollection.remove(new BasicDBObject("id", sampleId), null).getResult().get(0);
        List<Integer> deletes = new LinkedList<>();
        if (id.getN() == 0) {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        } else {
            deletes.add(id.getN());
            return endQuery("delete sample", startTime, deletes);
        }
    }

    @Override
//<<<<<<< HEAD
//    public QueryResult<Job> getJob(int jobId) throws CatalogManagerException {
//        long startTime = startQuery();
//        QueryResult queryResult = jobCollection.find(new BasicDBObject("id", jobId), null);
//        Job job = parseJob(queryResult);
//        if(job != null) {
//            return endQuery("Get job", startTime, Arrays.asList(job));
//=======
    public int getStudyIdBySampleId(int sampleId) throws CatalogDBException {
        DBObject query = new BasicDBObject("id", sampleId);
        BasicDBObject projection = new BasicDBObject(_STUDY_ID, true);
        QueryResult<DBObject> queryResult = sampleCollection.find(query, projection, null);
        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(_STUDY_ID);
            return studyId instanceof Integer ? (Integer) studyId : Integer.parseInt(studyId.toString());
//>>>>>>> bba62bea67b13e466ff74c6c0befb010e6fd05db
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    @Override
    public QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException {
        long startTime = startQuery();
//<<<<<<< HEAD
//        QueryResult queryResult = jobCollection.find(new BasicDBObject("analysisId", analysisId), null);
//        List<Job> jobs = parseJobs(queryResult);
//        return endQuery("Get all jobs", startTime, jobs);
//    }
//=======
        checkStudyId(studyId);
//>>>>>>> bba62bea67b13e466ff74c6c0befb010e6fd05db

        QueryResult<Long> count = studyCollection.count(BasicDBObjectBuilder
                .start(_ID, studyId)
                .append("cohorts.name", cohort.getName())
                .get());

        if(count.getResult().get(0) > 0) {
            throw new CatalogDBException("Cohort { name: \"" + cohort.getName() + "\" } already exists in this study.");
        }

        int newId = getNewId();
        cohort.setId(newId);

        DBObject cohortObject = getDbObject(cohort, "Cohort");
        QueryResult<WriteResult> update = studyCollection.update(
                new BasicDBObject(_ID, studyId),
                new BasicDBObject("$push", new BasicDBObject("cohorts", cohortObject)), null);

        if (update.getResult().get(0).getN() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        return endQuery("createDataset", startTime, getCohort(newId));
    }

    @Override
    public QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject("cohorts.id", cohortId);
        BasicDBObject projection = new BasicDBObject("cohorts", new BasicDBObject("$elemMatch", new BasicDBObject("id", cohortId)));
        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, null);

        List<Study> studies = parseStudies(queryResult);
        if(studies == null || studies.get(0).getCohorts().isEmpty()) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            return endQuery("getCohort", startTime, studies.get(0).getCohorts());
        }
    }

    @Override
    public int getStudyIdByCohortId(int cohortId) throws CatalogDBException {
        BasicDBObject query = new BasicDBObject("cohorts.id", cohortId);
        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);
        if(queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsField("id")) {
            throw CatalogDBException.idNotFound("Cohort", cohortId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Integer ? (Integer) id : Integer.parseInt(id.toString());
        }
    }

    /**
     * Annotation Methods
     * ***************************
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = studyCollection.count(
                new BasicDBObject("variableSets.name", variableSet.getName()).append("id", studyId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("VariableSet { name: '" + variableSet.getName() + "'} already exists.");
        }

        int variableSetId = getNewId();
        variableSet.setId(variableSetId);
        DBObject object = getDbObject(variableSet, "VariableSet");
        DBObject query = new BasicDBObject("id", studyId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject("variableSets", object));

        QueryResult<WriteResult> queryResult = studyCollection.update(query, update, null);

        return endQuery("createVariableSet", startTime, getVariableSet(variableSetId, null));
    }

    @Override
    public QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_STUDIES);
        DBObject query = new BasicDBObject("variableSets.id", variableSetId);
        DBObject projection = new BasicDBObject(
                "variableSets",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("id", variableSetId)
                )
        );
        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, filteredOptions);
        List<Study> studies = parseStudies(queryResult);
        if(studies.isEmpty() || studies.get(0).getVariableSets().isEmpty()) {
            throw new CatalogDBException("VariableSet {id: " + variableSetId + "} does not exist");
        }

        return endQuery("", startTime, studies.get(0).getVariableSets());
    }

    @Override
    public QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append("id", sampleId));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("AnnotationSet { id: " + annotationSet.getId() + "} already exists.");
        }

        DBObject object = getDbObject(annotationSet, "AnnotationSet");
        Map<String, String> annotationMap = new HashMap<>();
        for (Annotation annotation : annotationSet.getAnnotations()) {
            annotationMap.put(annotation.getId(), annotation.getValue().toString());
        }
        object.put("_annotMap", annotationMap);

        DBObject query = new BasicDBObject("id", sampleId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject("annotationSets", object));

        QueryResult<WriteResult> queryResult = sampleCollection.update(query, update, null);

        return endQuery("", startTime, Arrays.asList(annotationSet));
    }


    @Override
    public int getStudyIdByVariableSetId(int variableSetId) throws CatalogDBException {
        DBObject query = new BasicDBObject("variableSets.id", variableSetId);

        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", true), null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get("id");
            return studyId instanceof Integer ? (Integer) studyId : Integer.parseInt(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("VariableSet", variableSetId);
        }
    }


    /*
    * Helper methods
    ********************/

    //Join fields from other collections
    private void joinFields(User user, QueryOptions options) throws CatalogDBException {
        if (options == null) {
            return;
        }
        if (user.getProjects() != null) {
            for (Project project : user.getProjects()) {
                joinFields(project, options);
            }
        }
    }

    private void joinFields(Project project, QueryOptions options) throws CatalogDBException {
        if (options == null) {
            return;
        }
        if (options.getBoolean("includeStudies")) {
            project.setStudies(getAllStudies(project.getId(), options).getResult());
        }
    }

    private void joinFields(Study study, QueryOptions options) throws CatalogDBException {
        study.setDiskUsage(getDiskUsageByStudy(study.getId()));

        if (options == null) {
            return;
        }
        if (options.getBoolean("includeFiles")) {
            study.setFiles(getAllFiles(study.getId(), options).getResult());
        }
        if (options.getBoolean("includeJobs")) {
            study.setJobs(getAllJobs(study.getId(), options).getResult());
        }
        if (options.getBoolean("includeSamples")) {
            study.setSamples(getAllSamples(study.getId(), options).getResult());
        }
    }

    private User parseUser(QueryResult<DBObject> result) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonUserReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing user", e);
        }
    }

    private List<Study> parseStudies(QueryResult<DBObject> result) throws CatalogDBException {
        List<Study> studies = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                studies.add(jsonStudyReader.<Study>readValue(restoreDotsInKeys(object).toString()));
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
            return jsonFileReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing file", e);
        }
    }

    private List<File> parseFiles(QueryResult<DBObject> result) throws CatalogDBException {
        List<File> files = new LinkedList<>();
        try {
            for (DBObject o : result.getResult()) {
                files.add(jsonFileReader.<File>readValue(restoreDotsInKeys(o).toString()));
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
            return jsonJobReader.readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
    }

    private List<Job> parseJobs(QueryResult<DBObject> result) throws CatalogDBException {
        LinkedList<Job> jobs = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                jobs.add(jsonJobReader.<Job>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing job", e);
        }
        return jobs;
    }

    private List<Sample> parseSamples(QueryResult<DBObject> result) throws CatalogDBException {
        LinkedList<Sample> samples = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                samples.add(jsonSampleReader.<Sample>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing samples", e);
        }
        return samples;
    }

    private <T> List<T> parseObjects(QueryResult<DBObject> result, Class<T> tClass) throws CatalogDBException {
        LinkedList<T> objects = new LinkedList<>();
        try {
            for (DBObject object : result.getResult()) {
                objects.add(jsonReaderMap.get(tClass).<T>readValue(restoreDotsInKeys(object).toString()));
            }
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
        return objects;
    }

    private <T> T parseObject(QueryResult<DBObject> result, Class<T> tClass) throws CatalogDBException {
        if(result.getResult().isEmpty()) {
            return null;
        }
        try {
            return jsonReaderMap.get(tClass).readValue(restoreDotsInKeys(result.first()).toString());
        } catch (IOException e) {
            throw new CatalogDBException("Error parsing " + tClass.getName(), e);
        }
    }

    private DBObject getDbObject(Object object, String objectName) throws CatalogDBException {
        DBObject dbObject;
        try {
            dbObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(object));
            dbObject = replaceDotsInKeys(dbObject);
        } catch (Exception e) {
            throw new CatalogDBException("Error while writing to Json : " + objectName);
        }
        return dbObject;
    }

    static final String TO_REPLACE_DOTS = "&#46;";
//    static final String TO_REPLACE_DOTS = "\uff0e";

    /**
     * Scan all the DBObject and replace all the dots in keys with
     * @param object
     * @return
     */


    static <T> T replaceDotsInKeys(T object) {
        return replaceInKeys(object, ".", TO_REPLACE_DOTS);
    }
    static <T> T restoreDotsInKeys(T object) {
        return replaceInKeys(object, TO_REPLACE_DOTS, ".");
    }
    static <T> T replaceInKeys(T object, String target, String replacement) {
        if (object instanceof DBObject) {
            DBObject dbObject = (DBObject) object;
            List<String> keys = new ArrayList<>();
            for (String s : dbObject.keySet()) {
                if (s.contains(target)) {
                    keys.add(s);
                }
                replaceInKeys(dbObject.get(s), target, replacement);
            }
            for (String key : keys) {
                Object value = dbObject.removeField(key);
                key = key.replace(target, replacement);
                dbObject.put(key, value);
            }
        } else if (object instanceof List) {
            for (Object o : ((List) object)) {
                replaceInKeys(o, target, replacement);
            }
        }
        return object;
    }

    /**
     * Filter "include" and "exclude" options.
     *
     * Include and Exclude options are as absolute routes. This method removes all the values that are not in the
     * specified route. For the values in the route, the route is removed.
     *
     * [
     *  name,
     *  projects.id,
     *  projects.studies.id,
     *  projects.studies.alias,
     *  projects.studies.name
     * ]
     *
     * with route = "projects.studies.", then
     *
     * [
     *  id,
     *  alias,
     *  name
     * ]
     *
     * @param options
     * @param route
     * @return
     */
    private QueryOptions filterOptions(QueryOptions options, String route) {
        if(options == null) {
            return null;
        }

        QueryOptions filteredOptions = new QueryOptions(options); //copy queryOptions

        String[] filteringLists = {"include", "exclude"};
        for (String listName : filteringLists) {
            List<String> list = filteredOptions.getAsStringList(listName);
            List<String> filteredList = new LinkedList<>();
            int length = route.length();
            if(list != null) {
                for (String s : list) {
                    if(s.startsWith(route)) {
                        filteredList.add(s.substring(length));
                    }
                }
                filteredOptions.put(listName, filteredList);
            }
        }
        return filteredOptions;
    }


    /*  */

    private void addQueryStringListFilter(String key, QueryOptions options, DBObject query) {
        addQueryStringListFilter(key, options, key, query);
    }

    private void addQueryStringListFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) {
        if (options.containsKey(optionKey)) {
            List<String> stringList = options.getAsStringList(optionKey);
            if (stringList.size() > 1) {
                query.put(queryKey, new BasicDBObject("$in", stringList));
            } else if (stringList.size() == 1) {
                query.put(queryKey, stringList.get(0));
            }
        }
    }

    private void addQueryIntegerListFilter(String key, QueryOptions options, DBObject query) {
        addQueryIntegerListFilter(key, options, key, query);
    }

    private void addQueryIntegerListFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) {
        if (options.containsKey(optionKey)) {
            List<Integer> integerList = options.getAsIntegerList(optionKey);
            if (integerList.size() > 1) {
                query.put(queryKey, new BasicDBObject("$in", integerList));
            } else if (integerList.size() == 1) {
                query.put(queryKey, integerList.get(0));
            }
        }
    }

    static Pattern operationPattern = Pattern.compile("(.*)(<=?|>=?|!=|!?~=?|==)(.*)");
    private DBObject addCompQueryFilter(String optionKey, QueryOptions options, String queryKey, DBObject query) throws CatalogDBException {
        List<String> list = options.getAsStringList(optionKey);
        for (String cond : list) {
            Matcher matcher = operationPattern.matcher(cond);
            if (!matcher.find()) {
                throw new CatalogDBException("Unknown filter operation: " + cond);
            }
            addCompQueryFilter(
                    queryKey == null || queryKey.isEmpty()? matcher.group(1) : queryKey + "." + matcher.group(1),
                    matcher.group(2),
                    matcher.group(3), query);
        }
        return query;
    }

    private DBObject addCompQueryFilter(String queryKey, String op, String filter, DBObject query) throws CatalogDBException {
        try {
            switch (op) {
                case "<":
                    query.put(queryKey, new BasicDBObject("$lt", Float.parseFloat(filter)));
                    break;
                case "<=":
                    query.put(queryKey, new BasicDBObject("$lte", Float.parseFloat(filter)));
                    break;
                case ">":
                    query.put(queryKey, new BasicDBObject("$gt", Float.parseFloat(filter)));
                    break;
                case ">=":
                    query.put(queryKey, new BasicDBObject("$gte", Float.parseFloat(filter)));
                    break;
                case "==":
                    query.put(queryKey, new BasicDBObject("$eq", Float.parseFloat(filter)));
                    break;
                case "!=":
                    query.put(queryKey, new BasicDBObject("$ne", Float.parseFloat(filter)));
                    break;
                case "!~":
                case "!~=":
                    query.put(queryKey, new BasicDBObject("$not", new BasicDBObject("$regex", filter)));
                    break;
                case "~":
                case "~=":
                    query.put(queryKey, new BasicDBObject("$regex", filter));
                    break;
            }
        } catch (NumberFormatException e) {
            throw new CatalogDBException(e);
        }
        return query;
    }


    /*  */

    private void filterStringParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedParams) {
        for (String s : acceptedParams) {
            if(parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getString(s));
            }
        }
    }

    private void filterIntegerListParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntegerListParams) {
        for (String s : acceptedIntegerListParams) {
            if(parameters.containsKey(s)) {
                filteredParams.put(s, parameters.getAsIntegerList(s));
            }
        }
    }

    private void filterMapParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
        for (String s : acceptedMapParams) {
            if (parameters.containsKey(s)) {
                ObjectMap map;
                if (parameters.get(s) instanceof Map) {
                    map = new ObjectMap(parameters.getMap(s));
                } else {
                    map = new ObjectMap(parameters.getString(s));
                }
                try {
                    DBObject dbObject = getDbObject(map, s);
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        filteredParams.put(s + "." + entry.getKey(), dbObject.get(entry.getKey()));
                    }
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void filterObjectParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedMapParams) {
        for (String s : acceptedMapParams) {
            if (parameters.containsKey(s)) {
                DBObject dbObject = null;
                try {
                    dbObject = getDbObject(parameters.get(s), s);
                    filteredParams.put(s , dbObject);
                } catch (CatalogDBException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void filterIntParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedIntParams) {
        for (String s : acceptedIntParams) {
            if(parameters.containsKey(s)) {
                int anInt = parameters.getInt(s, Integer.MIN_VALUE);
                if(anInt != Integer.MIN_VALUE) {
                    filteredParams.put(s, anInt);
                }
            }
        }
    }

    private void filterLongParams(ObjectMap parameters, Map<String, Object> filteredParams, String[] acceptedLongParams) {
        for (String s : acceptedLongParams) {
            if(parameters.containsKey(s)) {
                long aLong = parameters.getLong(s, Long.MIN_VALUE);
                if (aLong != Long.MIN_VALUE) {
                    filteredParams.put(s, aLong);
                }
            }
        }
    }

}
