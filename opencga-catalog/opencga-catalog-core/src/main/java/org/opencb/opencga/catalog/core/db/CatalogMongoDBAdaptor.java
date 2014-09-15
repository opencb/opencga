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
import org.opencb.opencga.catalog.core.db.converters.DBObjectToFileConverter;
import org.opencb.opencga.catalog.core.db.converters.DBObjectToListConverter;
import org.opencb.opencga.catalog.core.db.converters.DBObjectToProjectConverter;
import org.opencb.opencga.catalog.core.db.converters.DBObjectToStudyConverter;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
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

    public static final String METADATA_OBJECT = "METADATA";

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

    private final DBObjectToProjectConverter projectConverter = new DBObjectToProjectConverter();
    private final DBObjectToListConverter<Project> projectListConverter = new DBObjectToListConverter<>(
            projectConverter, "projects");
    private final DBObjectToStudyConverter studyConverter = new DBObjectToStudyConverter();
    private final DBObjectToListConverter<Study> studyListConverter = new DBObjectToListConverter<>(
            studyConverter, "projects", "studies");
    private final DBObjectToFileConverter fileConverter = new DBObjectToFileConverter();

    private long startTime = 0;

    protected static Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);
    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;
    private Properties catalogProperties;

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
                projectConverter,
                BasicDBObjectBuilder.start("projects.id", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", project))).get()
        );
        Project p = (Project) queryResult.getResult().get(0);
        if (p == null) {
            return -1;
        } else {
            return p.getId();
            //return ((Integer) ((DBObject) ((BasicDBList) ((DBObject) queryResult.getResult().get(0)).get("projects")).get(0)).get("id"));
        }
    }


    /**
     * User methods ···
     * ***************************
     */

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
        startQuery();

        String userIdBySessionId = getUserIdBySessionId(sessionId);
        if(userIdBySessionId.equals(userId)){
            userCollection.update(
                    new BasicDBObject("sessions.id", sessionId),
                    new BasicDBObject("$set", new BasicDBObject("sessions.$.logout", TimeUtils.getTime())),
                    false,
                    false);

        } else {
            return endQuery("Logout", "UserId mismatches with the sessionId", null);
        }

        return null;
    }

    @Override
    public QueryResult loginAsAnonymous(Session session) throws CatalogManagerException, IOException {
        startQuery();

        QueryResult<Long> countSessions = userCollection.count(new BasicDBObject("sessions.id", session.getId()));
        if(countSessions.getResult().get(0) != 0){
            endQuery("Login as anonymous", "Error, sessionID already exists", null);
        }
        User user = new User("anonymous", "Anonymous", "", "", "", User.ROLE_ANONYMOUS, "");
        user.getSessions().add(session);
        DBObject anonymous = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(user));
        return endQuery("Login as anonymous", userCollection.insert(anonymous));
    }

    @Override
    public QueryResult logoutAnonymous(String sessionId) {
        return null;
    }

    @Override
    public QueryResult getUser(String userId, String lastActivity, String sessionId) throws CatalogManagerException {
        startQuery();
        //TODO: ManageSession
        DBObject query = new BasicDBObject("id", userId);
        return endQuery("get user", userCollection.find(query, null, null));
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
            return ((String) ((DBObject) id.getResult().get(0)).get("id"));
        } else {
            return null;
        }
    }


    /**
     * Project methods ···
     * ***************************
     */

    @Override
    public QueryResult createProject(String userId, Project project, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();
        //TODO: ManageSession and ACLs

        if(project.getStudies() != null && !project.getStudies().isEmpty()){
            return endQuery("Create Project", "Can't create project with studies", null);
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
        DBObject update = new BasicDBObject("$push", new BasicDBObject ("projects", JSON.parse(jsonObjectWriter.writeValueAsString(project))));
        System.out.println(update);
        return endQuery("Create Project", userCollection.update(query, update, false, false));
    }

    @Override
    public QueryResult getProject(String userId, String project, String sessionId) throws CatalogManagerException {
        startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject(
                "projects",
                new BasicDBObject(
                        "$elemMatch",
                        new BasicDBObject("alias", project)
                )
        );
        QueryResult queryResult = endQuery("get project", userCollection.find(
                query,
                null,
                        projectConverter,
                projection)
        );
        //List projects = (List) (((DBObject) (queryResult.getResult().get(0))).get("projects"));

        //queryResult.setResult(projects);
        System.out.println(queryResult);
        return queryResult;
    }

    @Override
    public QueryResult getAllProjects(String userId, String sessionId) throws CatalogManagerException {
        startQuery();
        //TODO: ManageSession

        DBObject query = new BasicDBObject("id", userId);
        DBObject projection = new BasicDBObject("projects", true); projection.put("_id", false);
        return endQuery(
                "User projects list",
                userCollection.find(
                        query,
                        null,
                        projectListConverter,
                        projection));
    }

    /**
     * Study methods ···
     * ***************************
     */

    @Override
    public QueryResult createStudy(String userId, String project, Study study, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();
        //TODO: ManageSession

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
                String userIdBySessionId = getUserIdBySessionId(sessionId);
                if(userIdBySessionId == null){
                    endQuery("Create Study", "Invalid session", null);
                }
                study.setCreatorId(userIdBySessionId);
            }
            DBObject query = new BasicDBObject("projects.id", projectId);
            DBObject update = new BasicDBObject("$push", new BasicDBObject(
                    "projects.$.studies", JSON.parse(jsonObjectWriter.writeValueAsString(study))
            ));
            System.out.println(update);
            return endQuery("Create Project", userCollection.update(query, update, false, false));
        }
    }

    @Override
    public QueryResult getAllStudies(String userId, String project, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();
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
        QueryResult<List<Study>> queryResult = (QueryResult<List<Study>>) endQuery("get project", userCollection.find(
                query,
                null,
                studyListConverter,
                projection));
        for (Study study : queryResult.getResult().get(0)) {
            study.setDiskUsage(getDiskUsageByStudy(study.getId(), sessionId));
        }
        return endQuery("Get all studies", queryResult);
    }

    @Override
    public QueryResult getStudy(int studyId, String sessionId) throws CatalogManagerException, JsonProcessingException{
        return null;
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
    public int getStudyId(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException {
        //TODO: ManageSession
        QueryResult queryResult = userCollection.find(
                BasicDBObjectBuilder
                        .start("projects.alias", projectAlias)
                        .append("projects.studies.alias", studyAlias)
                        .append("id", userId).get(),
                null,
                studyListConverter,
                BasicDBObjectBuilder
                        .start("projects.studies.id", true)
                        .append("projects.studies.alias", true)
                        .append("projects", new BasicDBObject("$elemMatch", new BasicDBObject("alias", projectAlias)))
                        .get()
        );
        if (queryResult.getNumResults() != 1) {
            return -1;
        } else {
            List<Study> studies = (List<Study>) queryResult.getResult().get(0);
            for (Study s : studies) {
                if(s.getAlias().equals(studyAlias)){
                    return s.getId();
                }
            }
            return -1;
//            return ((Integer) ((DBObject) ((BasicDBList) ((DBObject) queryResult.getResult().get(0)).get("projects")).get(0)).get("id"));
        }

    }

    /**
     * File methods ···
     * ***************************
     */

    @Override
    public QueryResult createFileToStudy(String userId, String projectAlias, String studyAlias, File file, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();
        //TODO: ManageSession

        int studyId = getStudyId(userId, projectAlias, studyAlias, sessionId);
        if(studyId < 0){
            return endQuery("Create file", "Study not exists", null);
        }
        file.setStudyId(studyId);
        file.setId(getNewFileId());
        file.setCreatorId(getUserIdBySessionId(sessionId));

        return endQuery("Create file", fileCollection.insert((DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(file))));
    }

    @Override
    public QueryResult createFileToStudy(int studyId, File file, String sessionId) throws CatalogManagerException, JsonProcessingException {
        startQuery();
        //TODO: ManageSession

        file.setStudyId(studyId);
        file.setId(getNewFileId());
        file.setCreatorId(getUserIdBySessionId(sessionId));

        return endQuery("Create file", fileCollection.insert((DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(file))));
    }

    @Override
    public QueryResult deleteFile(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException {
        return deleteFile(getFileId(userId, projectAlias, studyAlias, filePath, sessionId), sessionId);
    }

    @Override
    public QueryResult deleteFile(int studyId, Path filePath, String sessionId) throws CatalogManagerException {
        return deleteFile(getFileId(studyId, filePath, sessionId), sessionId);
    }

    @Override
    public QueryResult deleteFile(int fileId, String sessionId) throws CatalogManagerException {
        startQuery();

        WriteResult id = nativeFileCollection.remove(new BasicDBObject("id", fileId));
        if(id.getN() == 0){
            return endQuery("Delete file", "file not found", null);
        } else {
            return endQuery("Delete file", Arrays.asList(id.getN()));
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
    public int getFileId(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException {
        int studyId = getStudyId(userId, projectAlias, studyAlias, sessionId);
        return getFileId(studyId, filePath, sessionId);
    }

    @Override
    public int getFileId(int studyId, Path filePath, String sessionId) throws CatalogManagerException {

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("studyId", studyId)
                        .append("uri", filePath.toString()).get(),
                null,
                fileConverter,
                new BasicDBObject("id", true)
        );

        if(queryResult.getNumResults() != 0) {
            return ((File) queryResult.getResult().get(0)).getId();
        } else {
            return -1;
        }
    }

    @Override
    public QueryResult getFile(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException, IOException {
        return getFile(getStudyId(userId, projectAlias, studyAlias, sessionId), filePath, sessionId);
    }

    @Override
    public QueryResult getFile(int studyId, Path filePath, String sessionId) throws CatalogManagerException, IOException {
        startQuery();

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("studyId", studyId)
                        .append("uri", filePath.toString()).get(),
                null,
                fileConverter,
                null
        );

        if(queryResult.getNumResults() != 0) {
            return endQuery("Get file", queryResult);
        } else {
            return endQuery("Get file", "File not found", null);
        }
    }

    @Override
    public QueryResult getFile(int fileId, String sessionId) throws CatalogManagerException, IOException {
        startQuery();

        QueryResult queryResult = fileCollection.find(
                BasicDBObjectBuilder
                        .start("id", fileId).get(),
                null,
                fileConverter,
                null
        );

        if(queryResult.getNumResults() != 0) {
            return endQuery("Get file", queryResult);
        } else {
            return endQuery("Get file", "File not found", null);
        }
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
     * Analysis methods ···
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



}
