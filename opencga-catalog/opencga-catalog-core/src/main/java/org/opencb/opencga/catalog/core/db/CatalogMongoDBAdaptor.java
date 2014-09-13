package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.core.beans.*;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor implements CatalogDBAdaptor {

    public static final String METADATA_COLLECTION = "metadata";
    public static final String USER_COLLECTION = "user";
    public static final String FILE_COLLECTION = "file";
    public static final String SAMPLE_COLLECTION = "sample";
    public static final String JOB_COLLECTION = "job";
    public static final boolean LOGIN_AT_CREATE_USER = false;
    public static final String METADATA_OBJECT = "METADATA";

    private final MongoDataStoreManager mongoManager;
    private final MongoCredentials credentials;

    private MongoDataStore db;
    private MongoDBCollection metaCollection;
    private MongoDBCollection userCollection;
    private MongoDBCollection fileCollection;
    private MongoDBCollection sampleCollection;
    private MongoDBCollection jobCollection;

    private long startTime = 0;

    protected static Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);
    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;
    private Properties catalogProperties;
    private DBCollection nativeMetaCollection;

    public CatalogMongoDBAdaptor(MongoCredentials credentials) {
        this.mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        this.credentials = credentials;
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonObjectWriter = jsonObjectMapper.writer();

        //catalogProperties = Config.getAccountProperties();
    }

    public void connect() throws JsonProcessingException {
        db = mongoManager.get(credentials.getMongoDbName());
        nativeMetaCollection = db.getDb().getCollection(METADATA_COLLECTION);
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
                metadataObject.put("_id", METADATA_OBJECT);
                metaCollection.insert(metadataObject);
            } catch (MongoException.DuplicateKey e){
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
        }
    }


    public void disconnect(){
         mongoManager.close(db.getDatabaseName());
    }

    private void startQuery(){
        startTime = System.currentTimeMillis();
    }

    private QueryResult endQuery(String queryId, List result) {
        return endQuery(queryId, result, null, null);
    }

    private QueryResult endQuery(String queryId, QueryResult result) {
        long end = System.currentTimeMillis();
        result.setId(queryId);
        result.setDbTime((int)(end-startTime));
        return result;
    }

    private QueryResult endQuery(String queryId, String errorMessage, String warnMessage) {
        return endQuery(queryId, Collections.emptyList(), errorMessage, warnMessage);
    }

    private QueryResult endQuery(String queryId, List result, String errorMessage, String warnMessage){
        long end = System.currentTimeMillis();
        int numResults = result != null ? result.size() : 0;

        return new QueryResult(queryId, (int) (end-startTime), numResults, numResults, warnMessage, errorMessage, result);
    }
    /*
        Auxyliar query methods
     */


    private boolean userExists(String userId){
        QueryResult count = userCollection.count(new BasicDBObject("id", userId));
        long l = (Long) count.getResult().get(0);
        return l != 0;
    }

    @Override
    public String getUserIdBySessionId(String sessionId){
        QueryResult id = userCollection.find(new BasicDBObject("sessions.id", sessionId), null, null, new BasicDBObject("id", true));

        if (id.getNumResults() != 0) {
            return ((String) ((DBObject) id.getResult().get(0)).get("id"));
        } else {
            return null;
        }
    }


    private int getNewProjectId()  {return getNewId("projectCounter");}
    private int getNewStudyId()    {return getNewId("studyCounter");}
    private int getNewFileId()     {return getNewId("fileCounter");}
    private int getNewAnalysisId() {return getNewId("analysisCounter");}
    private int getNewJobId()      {return getNewId("jobCounter");}
    private int getNewSampleId()   {return getNewId("sampleCounter");}

    private int getNewId(String field){
        DBObject object = nativeMetaCollection.findAndModify(
                new BasicDBObject("_id", METADATA_OBJECT),  //Query
                new BasicDBObject(field, true),  //Fields
                null,
                false,
                new BasicDBObject("$inc", new BasicDBObject(field, 1)), //Update
                true,
                false
        );
        return (int)(Float.parseFloat(object.get(field).toString()));
    }

    public int getProjectId(String userId, String project) {
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", project)
                        .append("id", userId).get(),
                null,
                null,
                new BasicDBObject("projects.id", true)
        );
        if (queryResult.getNumResults() != 1) {
            return -1;
        } else {
            return ((Integer) ((DBObject) ((BasicDBList) ((DBObject) queryResult.getResult().get(0)).get("projects")).get(0)).get("id"));
        }
    }
    @Override
    public boolean checkUserCredentials(String userId, String sessionId) {
        return false;
    }

    @Override
    public QueryResult createUser(User user) throws CatalogManagerException, JsonProcessingException {
        startQuery();

        //TODO: ManageSession

        user.setAnalyses(Collections.<Analysis>emptyList());
        user.setProjects(Collections.<Project>emptyList());
        user.setPlugins(Collections.<Tool>emptyList());
        user.setSessions(Collections.<Session>emptyList());

        if(userExists(user.getId())){
            return endQuery("createUser", "UserID already exists", null);
        }
        QueryResult insert = userCollection.insert((DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user)));

        return endQuery("createUser", insert);
    }

    @Override
    public QueryResult deleteUser(String userId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }

    @Override
    public QueryResult login(String userId, String password, Session session) throws CatalogManagerException, IOException {
        startQuery();

        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder.start("id", userId).append("password", password).get());
        if(count.getResult().get(0) == 0){
            return endQuery("Login", "Bad user or password", null);
        } else {
            QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
            if (countSessions.getResult().get(0) != 0) {
                return endQuery("Login", "Already loggin", null);
            } else {
                BasicDBObject id = new BasicDBObject("id", userId);
                BasicDBObject updates = new BasicDBObject(
                        "$push", new BasicDBObject(
                                "sessions", (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(session))
                        )
                );
                return endQuery("Login", userCollection.update(id, updates, false, false));
            }
        }
    }

    @Override
    public QueryResult logout(String userId, String sessionId) throws CatalogManagerException, IOException {
        return null;
    }

    @Override
    public QueryResult loginAsAnonymous(Session session) throws CatalogManagerException, IOException {
        startQuery();

        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if(countSessions.getResult().get(0) != 0){
            endQuery("Login as anonymous", "Error, sessionID already exists", null);
        }
        User user = new User("anonymous", "Anonymous", "no@email.com", "", "ACME", User.ROLE_ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
        return endQuery("Login as anonymous", userCollection.insert(anonymous));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) {
        return null;
    }

    @Override
    public QueryResult getUser(String userId, String sessionId, String lastActivity) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult changePassword(String userId, String sessionId, String password, String password1) throws CatalogManagerException {
        startQuery();
        //TODO: ManageSession

        BasicDBObject query = new BasicDBObject("id", userId);
        query.put("sessions.id", sessionId);
        query.put("password", password);
        BasicDBObject fields = new BasicDBObject("password", password1);
        BasicDBObject action = new BasicDBObject("$set", fields);

        return endQuery("Change Password", userCollection.update(query, action, false, false));

    }

    @Override
    public QueryResult changeEmail(String userId, String sessionId, String nEmail) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult resetPassword(String userId, String email) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult getAllProjects(String userId, String sessionId) throws CatalogManagerException {
        startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject("projects", true);
        return endQuery("User projects list",userCollection.find(query, null, null, projection));
    }

    @Override
    public QueryResult createProject(String userId, Project project, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();
        //TODO: ManageSession and ACLs

        if(project.getStudies() != null && !project.getStudies().isEmpty()){
            return endQuery("Create Project", "Can't create project with studyes", null);
        }
        QueryResult<Long> count = userCollection.count(BasicDBObjectBuilder
                .start("id", userId)
                .append("projects.alias", project.getAlias()).get());
        if(count.getResult().get(0) != 0){
            return endQuery("Create Project", "Project alias already exists in this user", null);
            //IDEA: For each study, "createStudy" or "Can't create study error"
        }

        project.setId(getNewProjectId());
        DBObject query = new BasicDBObject("id", userId);
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(project))));
        System.out.println(update);
        return endQuery("Create Project", userCollection.update(query, update, false, false));
    }

    @Override
    public QueryResult getProject(String userId, String project, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult getAnalysisList(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult getAnalysisList(int studyId, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }

    @Override
    public QueryResult createAnalysis(int studyId, Analysis analysis, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return null;
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

    @Override
    public QueryResult getSession(String userId, String sessionId) throws IOException {
        return null;
    }

    @Override
    public QueryResult getAllStudies(String userId, String project, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }



    @Override
    public QueryResult createStudy(String userId, String project, Study study, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();

        int projectId = getProjectId(userId, project);
        if(projectId < 0){
            return endQuery("Create Study", "Project not found", null);
        }
        QueryResult<Long> queryResult = userCollection.count(BasicDBObjectBuilder
                .start("projects.id", projectId)
                .append("projects.studies.alias", study.getAlias()).get());

        if (queryResult.getResult().get(0) != 0) {
            return endQuery("Create Study", "Study alias already exists", null);
        } else {
            study.setId(getNewStudyId());
            if (study.getCreatorId() == null) {
                study.setCreatorId(getUserIdBySessionId(sessionId));
            }
            DBObject query = new BasicDBObject("projects.id", projectId);
            DBObject update = new BasicDBObject("$push", new BasicDBObject(
                    "projects.$.studies", (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(study))
            ));
            System.out.println(update);
            return endQuery("Create Project", userCollection.update(query, update, false, false));
        }
    }

    @Override
    public QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudyName, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult renameStudy(int studyId, String newStudyName, String sessionId) throws CatalogManagerException {
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
    public int getStudyId(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException, IOException {
        return 0;
    }

    @Override
    public QueryResult createFileToStudy(String userId, String projectAlias, String studyAlias, File file, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }

    @Override
    public QueryResult createFileToStudy(int studyId, File file, String sessionId) throws CatalogManagerException, JsonProcessingException {
        return null;
    }

    @Override
    public QueryResult deleteFile(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult deleteFile(int studyId, Path filePath, String sessionId) throws CatalogManagerException {
        return null;
    }

    @Override
    public QueryResult deleteFile(int fileId, String sessionId) throws CatalogManagerException {
        return null;
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
    public int getFileId(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException, IOException {
        return 0;
    }

    @Override
    public int getFileId(int studyId, Path filePath, String sessionId) throws CatalogManagerException, IOException {
        return 0;
    }

    @Override
    public QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, Path filePath, String status, String sessionId) throws CatalogManagerException, IOException {
        return null;
    }

    @Override
    public QueryResult setFileStatus(int studyId, Path filePath, String status, String sessionId) throws CatalogManagerException, IOException {
        return null;
    }

    @Override
    public QueryResult setFileStatus(int fileId, String status, String sessionId) throws CatalogManagerException, IOException {
        return null;
    }

}
