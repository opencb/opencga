package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.*;
import com.mongodb.util.JSON;
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
import java.nio.file.Path;
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



    public CatalogMongoDBAdaptor(MongoCredentials credentials) {
        super();
        this.mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        this.credentials = credentials;
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonFileReader = jsonObjectMapper.reader(File.class);
        jsonUserReader = jsonObjectMapper.reader(User.class);
        jsonProjectReader = jsonObjectMapper.reader(Project.class);
        jsonStudyReader = jsonObjectMapper.reader(Study.class);
        jsonSampleReader = jsonObjectMapper.reader(Sample.class);
        //catalogProperties = Config.getAccountProperties();

        connect();
    }

    private void connect()  {
        db = mongoManager.get(credentials.getMongoDbName());
        nativeMetaCollection = db.getDb().getCollection(METADATA_COLLECTION);
        nativeFileCollection = db.getDb().getCollection(FILE_COLLECTION);

        metaCollection = db.getCollection(METADATA_COLLECTION);
        userCollection = db.getCollection(USER_COLLECTION);
        fileCollection = db.getCollection(FILE_COLLECTION);
        sampleCollection = db.getCollection(SAMPLE_COLLECTION);
        jobCollection = db.getCollection(JOB_COLLECTION);


        //If "metadata" document doesn't exist, create.
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_COLLECTION));
        if(queryResult.getNumResults() == 0){
            try {
                DBObject metadataObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(new Metadata()));
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject);
            } catch (MongoException.DuplicateKey e){
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            } catch (JsonProcessingException e) {
                logger.error("Metadata json parse error", e);
            }
        }
    }

    public void disconnect(){
         mongoManager.close(db.getDatabaseName());
    }

    private long startQuery(){
        return System.currentTimeMillis();
    }

    private <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result) {
        return endQuery(queryId, startTime, result, null, null);
    }

    private QueryResult endQuery(String queryId, long startTime, String errorMessage) {
        return endQuery(queryId, startTime, Collections.emptyList(), errorMessage, null);
    }

    //TODO: Shoud throw error if errorMessage != null?
    private <T> QueryResult<T> endQuery(String queryId, long startTime, QueryResult<T> result) {
        long end = System.currentTimeMillis();
        result.setId(queryId);
        result.setDbTime((int)(end-startTime));
        return result;
    }
    //TODO: Shoud throw error if errorMessage != null?
    private <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result, String errorMessage, String warnMessage){
        long end = System.currentTimeMillis();
        int numResults = result != null ? result.size() : 0;

        return new QueryResult<>(queryId, (int) (end-startTime), numResults, numResults, warnMessage, errorMessage, result);
    }
    /*
        Auxiliary query methods
     */


    private boolean userExists(String userId){
        QueryResult count = userCollection.count(new BasicDBObject("id", userId));
        long l = (Long) count.getResult().get(0);
        return l != 0;
    }



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

    public int getProjectId(String userId, String project) {
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", project)
                        .append("id", userId).get(),
                null,
                null,
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", project))).get()
        );
        if (queryResult.getNumResults() != 1) {
            return -1;
        } else {
            try {
                User user = jsonUserReader.readValue(queryResult.getResult().get(0).toString());
                return user.getProjects().get(0).getId();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return -1;
        }
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
    public QueryResult<User> createUser(User user) throws CatalogManagerException, JsonProcessingException {
        long startTime = startQuery();

        if(userExists(user.getId())){
            return endQuery("createUser", startTime, "UserID already exists");
        }
        List<Project> projects = user.getProjects();
        user.setProjects(Collections.<Project>emptyList());
        DBObject userDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));

        QueryResult insert = userCollection.insert(userDBObject);
        String errorMsg = insert.getErrorMsg() != null? insert.getErrorMsg() : "";
        for (Project p : projects){
            String projectErrorMsg = createProject(user.getId(), p).getErrorMsg();
            if(projectErrorMsg != null){
                errorMsg += ", " + p.getAlias() + ":" + projectErrorMsg;
            }
        }

        //Get the inserted user.
        List<User> result = getUser(user.getId(), "").getResult();

        return endQuery("createUser", startTime, result, errorMsg, null);
    }

    @Override
    public QueryResult deleteUser(String userId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }

    @Override
    public QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
        if(count.getResult().get(0) == 0){
            return endQuery("Login", startTime, "Bad user or password");
        } else {

            QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
            if (countSessions.getResult().get(0) != 0) {
                return endQuery("Login", startTime, "Already logged");
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
    public QueryResult logout(String userId, String sessionId) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        String userIdBySessionId = getUserIdBySessionId(sessionId);
        if(userIdBySessionId.equals(userId)){
            userCollection.update(
                    new BasicDBObject("sessions.id", sessionId),
                    new BasicDBObject("$set", new BasicDBObject("sessions.$.logout", TimeUtils.getTime())),
                    false,
                    false);

        } else {
            return endQuery("Logout", startTime, "UserId mismatches with the sessionId");
        }

        return null;
    }

    @Override
    public QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if(countSessions.getResult().get(0) != 0){
            endQuery("Login as anonymous", startTime, "Error, sessionID already exists");
        }
        String userId = "anonymous_" + session.getId();
        User user = new User(userId, "Anonymous", "", "", "", User.ROLE_ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
        userCollection.insert(anonymous);

        ObjectMap resultObjectMap = new ObjectMap();
        resultObjectMap.put("sessionId", session.getId());
        resultObjectMap.put("userId", userId);
        return endQuery("Login as anonymous", startTime, Arrays.asList(resultObjectMap));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) {
        return null;
    }

    @Override
    public QueryResult<User> getUser(String userId, String lastActivityg) throws CatalogManagerException{
        long startTime = startQuery();
        //TODO: ManageSession
        //TODO: Check "lastActivity". If lastActivity == user.getLastActivity, return []
        DBObject query = new BasicDBObject("id", userId);
        QueryResult result = userCollection.find(query, null, null);

        User user;
        try {
            user = jsonUserReader.readValue(result.getResult().get(0).toString());
        } catch (IOException e) {
            throw new CatalogManagerException("Fail to parse mongo : " + e.getMessage());
        }
        return endQuery("get user", startTime, Arrays.asList(user));
    }

    @Override
    public QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("password", oldPassword);
        BasicDBObject fields = new BasicDBObject("password", newPassword);
        BasicDBObject action = new BasicDBObject("$set", fields);

        return endQuery("Change Password", startTime, userCollection.update(query, action, false, false));

    }

    @Override
    public QueryResult changeEmail(String userId, String newEmail) throws CatalogManagerException {
        return null;
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

    @Override
    public QueryResult<Project> createProject(String userId, Project project) throws CatalogManagerException, JsonProcessingException {
        long startTime = startQuery();

        List<Study> studies = project.getStudies();
        if(studies == null) {
            studies = Collections.emptyList();
        }
        project.setStudies(Collections.<Study>emptyList());


        // Check if project.alias already exists.
        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder
                .start("id", userId)
                .append("projects.alias", project.getAlias()).get());
        if(count.getResult().get(0) != 0){
            return endQuery("Create Project", startTime, "Project alias already exists in this user");
        }

        int projectId = getNewProjectId();
        project.setId(projectId);
        DBObject query = new BasicDBObject("id", userId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", JSON.parse(jsonObjectWriter.writeValueAsString(project))));
        QueryResult queryResult = userCollection.update(query, update, false, false);

        for (Study study : studies) {
            String errorMsg = createStudy(userId, project.getAlias(), study).getErrorMsg();
            if(errorMsg != null){
                queryResult.setErrorMsg(queryResult.getErrorMsg() + ", " + study.getAlias() + ":" + errorMsg);
            }
        }
        List<Project> result = getProject(userId, project.getAlias()).getResult();
        return endQuery("Create Project", startTime, result, queryResult.getErrorMsg(), null);
    }

    @Override
    public QueryResult<Project> getProject(String userId, String project) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject(
                "projects",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("alias", project)
                )
        );
        QueryResult queryResult = endQuery("get project", startTime, userCollection.find(
                query,
                null,
                null,
                projection)
        );
        User user;
        try {
            user = jsonUserReader.readValue(queryResult.getResult().get(0).toString());
            queryResult.setResult(user.getProjects());
            return queryResult;
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogManagerException("Failed to parse mongo schema : " + e.getMessage());
        }
    }

    @Override
    public QueryResult<Project> getAllProjects(String userId, String sessionId) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject("projects", true); projection.put("_id", false);
        QueryResult result = userCollection.find(
                query,
                null,
                null,   //projectListConverter
                projection);
        User user;
        try {
            user = jsonUserReader.readValue(result.getResult().get(0).toString());
            return endQuery(
                    "User projects list", startTime,
                    user.getProjects());
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogManagerException("Fail to parse mongo : " + e.getMessage());
        }
    }

    @Override
    public QueryResult renameProject(String userId, String projectName, String newProjectName, String sessionId) throws CatalogManagerException {
        return null;
    }

    /**
     * Study methods
     * ***************************
     */
    @Override
    public QueryResult<Study> createStudy(String userId, String project, Study study) throws CatalogManagerException {
        long startTime = startQuery();
        int projectId = getProjectId(userId, project);
        if(projectId < 0){
            return endQuery("Create Study", startTime, "Project not found");
        }
        //TODO: remove files and replace them with ids
        //TODO: Generate default folders.

        // Check if study.alias already exists.
        QueryResult<Long> queryResult = userCollection.count(BasicDBObjectBuilder
                .start("projects.id", projectId)
                .append("projects.studies.alias", study.getAlias()).get());
        if (queryResult.getResult().get(0) != 0) {
            return endQuery("Create Study", startTime, "Study alias already exists");
        } else {
            study.setId(getNewStudyId());
            if (study.getCreatorId() == null) {
                study.setCreatorId(userId);
            }
            List<Analysis> analyses = study.getAnalyses();
            List<File> files = study.getFiles();
            study.setAnalyses(Collections.<Analysis>emptyList());
            study.setFiles(Collections.<File>emptyList());

            DBObject query = new BasicDBObject("projects.id", projectId);
            DBObject studyObject = null;
            try {
                studyObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(study));
            } catch (JsonProcessingException e) {
                throw new CatalogManagerException(e);
            }
            DBObject update = new BasicDBObject("$push", new BasicDBObject(
                    "projects.$.studies", studyObject ));
            QueryResult updateResult = userCollection.update(query, update, false, false);


            for(Analysis analysis : analyses){
                String errorMsg = null;
                try {
                    errorMsg = createAnalysis(study.getId(), analysis).getErrorMsg();
                } catch (JsonProcessingException e) {
                    throw new CatalogManagerException(e);
                }
                if(errorMsg != null){
                    updateResult.setErrorMsg(updateResult.getErrorMsg() + ", " + analysis.getAlias() + ":" + errorMsg);
                }
            }

            for (File file : files) {
                String errorMsg = null;
                try {
                    errorMsg = createFileToStudy(study.getId(), file).getErrorMsg();
                } catch (JsonProcessingException e) {
                    throw new CatalogManagerException(e);
                }
                if(errorMsg != null){
                    updateResult.setErrorMsg(updateResult.getErrorMsg() + ", " + file.getName() + ":" + errorMsg);
                }
            }


            return endQuery("Create Study", startTime, updateResult);
        }
    }

    @Override
    public QueryResult<Study> getAllStudies(String userId, String project, String sessionId) throws CatalogManagerException {
        long startTime = startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = BasicDBObjectBuilder
                .start(new BasicDBObject(
                                "projects",
                                new BasicDBObject(
                                        "$elemMatch",
                                        new BasicDBObject("alias", project)
                                )
                        )
                )
                .append("projects.studies", true).get();
        QueryResult queryResult = endQuery("get project", startTime, userCollection.find(
                query,
                null,
                null,
                projection));
        User user = null;
        try {
            user = jsonUserReader.readValue(queryResult.getResult().get(0).toString());

            List<Study> studies = user.getProjects().get(0).getStudies();
            for (Study study : studies) {
                study.setDiskUsage(getDiskUsageByStudy(study.getId(), sessionId));
            }

            //TODO: append files
            return endQuery("Get all studies", startTime, studies);

        } catch (IOException e) {
            throw new CatalogManagerException("GetAllStudies IOException", e);
        }
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, String sessionId) throws CatalogManagerException{
        long startTime = startQuery();
        //TODO: ManageSession
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

        try {
            User user = jsonUserReader.readValue(queryResult.getResult().get(0).toString());
            List<Study> studies = user.getProjects().get(0).getStudies();
            Study study = null;
            for (Study st : studies) {
                if (st.getId() == studyId) {
                    study = st;
                }
            }
            if (study != null) {
                study.setDiskUsage(getDiskUsageByStudy(study.getId(), sessionId));
            }
            // TODO study.setAnalysis
            // TODO study.setfiles
            studies = new LinkedList<>();
            studies.add(study);
            //queryResult.setResult(studies);
            return endQuery("Get Study", startTime, studies);
        } catch (IOException e) {
            e.printStackTrace();
            throw new CatalogManagerException("Get study IOException", e);
        }
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
    public QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudyName, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyName, String sessionId) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        QueryResult studyResult = getStudy(studyId, sessionId);
        return null;
    }

    @Override
    public QueryResult deleteStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult deleteStudy(int studyId, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public int getStudyId(String userId, String projectAlias, String studyAlias) throws CatalogManagerException, IOException {
        //TODO: ManageSession
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", projectAlias)
                        .append("projects.studies.alias", studyAlias)
                        .append("id", userId).get(),
                null,
                null,
                BasicDBObjectBuilder
                        .start("projects.studies.id", true)
                        .append("projects.studies.alias", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", projectAlias)))
                        .get()
        );
        if (queryResult.getNumResults() != 1) {
            return -1;
        } else {
            User user = jsonUserReader.readValue(queryResult.getResult().get(0).toString());
            for (Study s : user.getProjects().get(0).getStudies()) {
                if(s.getAlias().equals(studyAlias)){
                    return s.getId();
                }
            }
            return -1;
        }

    }

    /**
     * File methods
     * ***************************
     */

    @Override
    public QueryResult<File> createFileToStudy(String userId, String projectAlias, String studyAlias, File file) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        int studyId = getStudyId(userId, projectAlias, studyAlias);
        if(studyId < 0){
            return endQuery("Create file", startTime, "Study not exists");
        }
        QueryResult<File> fileToStudy = createFileToStudy(studyId, file);
        return endQuery("Create file", startTime, fileToStudy);
    }

    @Override
    public QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogManagerException, JsonProcessingException {
        long startTime = startQuery();

        int newFileId = getNewFileId();
        file.setId(newFileId);
        if(file.getCreatorId() == null) {
            file.setCreatorId(getUserIdByStudyId(studyId));
        }
        DBObject fileDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(file));
        fileDBObject.put("studyId", studyId);
        fileCollection.insert(fileDBObject);

        return endQuery("Create file", startTime, Arrays.asList(file));
    }


    @Override
    public QueryResult deleteFile(String userId, String projectAlias, String studyAlias, Path filePath) throws CatalogManagerException, IOException {
        return deleteFile(getFileId(userId, projectAlias, studyAlias, filePath));
    }

    @Override
    public QueryResult deleteFile(int studyId, Path filePath) throws CatalogManagerException {
        return deleteFile(getFileId(studyId, filePath));
    }

    @Override
    public QueryResult deleteFile(int fileId) throws CatalogManagerException {
        long startTime = startQuery();

        WriteResult id = nativeFileCollection.remove(new BasicDBObject("id", fileId));
        if(id.getN() == 0){
            return endQuery("Delete file", startTime, "file not found");
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
    public int getFileId(String userId, String projectAlias, String studyAlias, Path filePath) throws CatalogManagerException, IOException {
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        return getFileId(studyId, filePath);
    }

    @Override
    public int getFileId(int studyId, Path filePath) throws CatalogManagerException {

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("studyId", studyId)
                        .append("uri", filePath.toString()).get(),
                null,
                null,
                new BasicDBObject("id", true)
        );
        if(queryResult.getNumResults() != 0) {
            try {
                File file = jsonFileReader.readValue(queryResult.getResult().get(0).toString());
                return file.getId();
            } catch (IOException e) {
                throw new CatalogManagerException("GetFileId", e);
            }
        } else {
            return -1;
        }
    }

    @Override
    public QueryResult<File> getFile(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException, IOException {
        return getFile(getStudyId(userId, projectAlias, studyAlias), filePath, sessionId);
    }

    @Override
    public QueryResult<File> getFile(int studyId, Path filePath, String sessionId) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("studyId", studyId)
                        .append("uri", filePath.toString()).get(),
                null,
                null,
                null
        );

        if(queryResult.getNumResults() != 0) {
            File file = jsonFileReader.readValue(queryResult.getResult().get(0).toString());
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            return endQuery("Get file", startTime, "File not found");
        }
    }

    @Override
    public QueryResult<File> getFile(int fileId, String sessionId) throws CatalogManagerException, IOException {
        long startTime = startQuery();

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("id", fileId).get(),
                null,
                null,
                null
        );
//        List<File> files = new LinkedList<>();
//        for (DBObject object : (List<DBObject>)queryResult.getResult()) {
//            files.add((File) jsonFileReader.readValue(object.toString()));
//        }

        if(queryResult.getNumResults() != 0) {
            File file = jsonFileReader.readValue(queryResult.getResult().get(0).toString());
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            return endQuery("Get file", startTime, "File not found");
        }
    }

    @Override
    public QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, Path filePath, String status, String sessionId) throws CatalogManagerException, IOException {
        int fileId = getFileId(userId, projectAlias, studyAlias, filePath);
        return setFileStatus(fileId, status, sessionId);
    }

    @Override
    public QueryResult setFileStatus(int studyId, Path filePath, String status, String sessionId) throws CatalogManagerException, IOException {
        int fileId = getFileId(studyId, filePath);
        return setFileStatus(fileId, status, sessionId);
    }

    @Override
    public QueryResult setFileStatus(int fileId, String status, String sessionId) throws CatalogManagerException, IOException {
        long startTime = startQuery();
        System.out.println(fileId);
        QueryResult update = fileCollection.update(
                new BasicDBObject("id", fileId),
                new BasicDBObject("$set",
                        new BasicDBObject("status", status)),
                false,
                false);

        return endQuery("Set file status", startTime, update);
    }

    private int getDiskUsageByStudy(int studyId , String sessionId){
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
     * Analysis methods
     * ***************************
     */
    @Override
    public QueryResult getAnalysisList(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult getAnalysisList(int studyId, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis) throws CatalogManagerException, IOException {
        long startTime = startQuery();
        int studyId = getStudyId(userId, projectAlias, studyAlias);
        if (studyId < 0) {
            return endQuery("Create Analysis", startTime, "Study not found");
        } else {
            return createAnalysis(studyId, analysis);
        }
    }

    @Override
    public QueryResult createAnalysis(int studyId, Analysis analysis) throws CatalogManagerException, JsonProcessingException {
        long startTime = startQuery();
        // TODO manage session


        // Check if analysis.alias already exists.
        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder
                .start("projects.studies.id", studyId)
                .append("analysis.alias", analysis.getAlias()).get());
        if(count.getResult().get(0) != 0) {
            return endQuery("Create Analysis", startTime, "Analysis alias already exists in this study");
        }

        analysis.setId(getNewAnalysisId());

        List<Job> jobs = analysis.getJobs();
        if (jobs == null) {
            jobs = Collections.<Job>emptyList();
        }
        // TODO create jobs
        analysis.setJobs(Collections.<Job>emptyList()); // TODO revise if this can be used in mongo as a jobId list
        // TODO analysis set studyId. Analysis.studyId exists, but wasn't it removed?

        DBObject query = new BasicDBObject("projects.studies.id", studyId);
        DBObject analysisObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(analysis));
        DBObject update = new BasicDBObject("$push", new BasicDBObject("analysis", analysisObject));
        QueryResult updateResult = userCollection.update(query, update, false, false);

        return endQuery("Create Analysis", startTime, updateResult);    // TODO test
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

//    private User appendFilesToUser(User user, List<File> files) {
//
//    }

}
