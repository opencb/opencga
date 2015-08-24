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

import com.mongodb.*;
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

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.addCompQueryFilter;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor extends CatalogDBAdaptor
        implements CatalogDBAdaptorFactory, /*CatalogUserDBAdaptor, */CatalogStudyDBAdaptor, CatalogFileDBAdaptor, CatalogJobDBAdaptor/*, CatalogSampleDBAdaptor */{

    private static final String USER_COLLECTION = "user";
    private static final String STUDY_COLLECTION = "study";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String INDIVIDUAL_COLLECTION = "individual";
    private static final String METADATA_COLLECTION = "metadata";

    static final String METADATA_OBJECT_ID = "METADATA";

    //Keys to foreign objects.
    static final String _ID = "_id";
    static final String _PROJECT_ID = "_projectId";
    static final String _STUDY_ID = "_studyId";
    static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    static final String FILTER_ROUTE_INDIVIDUALS = "projects.studies.individuals.";
    static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    static final String FILTER_ROUTE_FILES =   "projects.studies.files.";
    static final String FILTER_ROUTE_JOBS =    "projects.studies.jobs.";

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
    private MongoDBCollection individualCollection;
    private MongoDBCollection jobCollection;
    private Map<String, MongoDBCollection> collections;
    private CatalogMongoUserDBAdaptor userDBAdaptor;
    private CatalogMongoIndividualDBAdaptor individualDBAdaptor;
    private CatalogMongoSampleDBAdaptor sampleDBAdaptor;

    //    private static final Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);



    @Override
    public CatalogUserDBAdaptor getCatalogUserDBAdaptor() {
        return userDBAdaptor;
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
        return sampleDBAdaptor;
    }

    @Override
    public CatalogIndividualDBAdaptor getCatalogIndividualDBAdaptor() {
        return individualDBAdaptor;
    }

    @Override
    public CatalogJobDBAdaptor getCatalogJobDBAdaptor() {
        return this;
    }


    public CatalogMongoDBAdaptor(List<DataStoreServerAddress> dataStoreServerAddressList, MongoDBConfiguration configuration, String database)
            throws CatalogDBException {
        super(LoggerFactory.getLogger(CatalogMongoDBAdaptor.class));
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddressList);
        this.configuration = configuration;
        this.database = database;

        connect();
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(database, configuration);
        if(db == null){
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        collections = new HashMap<>();
        collections.put(METADATA_COLLECTION, metaCollection = db.getCollection(METADATA_COLLECTION));
        collections.put(USER_COLLECTION, userCollection = db.getCollection(USER_COLLECTION));
        collections.put(STUDY_COLLECTION, studyCollection = db.getCollection(STUDY_COLLECTION));
        collections.put(FILE_COLLECTION, fileCollection = db.getCollection(FILE_COLLECTION));
        collections.put(SAMPLE_COLLECTION, sampleCollection = db.getCollection(SAMPLE_COLLECTION));
        collections.put(INDIVIDUAL_COLLECTION, individualCollection = db.getCollection(INDIVIDUAL_COLLECTION));
        collections.put(JOB_COLLECTION, jobCollection = db.getCollection(JOB_COLLECTION));

        userDBAdaptor = new CatalogMongoUserDBAdaptor(this, metaCollection, userCollection);
        individualDBAdaptor = new CatalogMongoIndividualDBAdaptor(this, metaCollection, individualCollection);
        sampleDBAdaptor = new CatalogMongoSampleDBAdaptor(this, metaCollection, sampleCollection, studyCollection);
    }

    @Override
    public void initializeCatalogDB() throws CatalogDBException {
        //If "metadata" document doesn't exist, create.
        if(!isCatalogDBReady()) {

            /* Check all collections are empty */
            for (Map.Entry<String, MongoDBCollection> entry : collections.entrySet()) {
                if (entry.getValue().count().first() != 0L) {
                    throw new CatalogDBException("Fail to initialize Catalog Database in MongoDB. Collection " + entry.getKey() + " is not empty.");
                }
            }

            try {
                DBObject metadataObject = getDbObject(new Metadata(), "Metadata");
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject, null);

            } catch (DuplicateKeyException e) {
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
            //Set indexes
//            BasicDBObject unique = new BasicDBObject("unique", true);
//            nativeUserCollection.createIndex(new BasicDBObject("id", 1), unique);
//            nativeFileCollection.createIndex(BasicDBObjectBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndex(new BasicDBObject("id", 1), unique);
        } else {
            throw new CatalogDBException("Catalog already initialized");
        }
    }

    /**
     * CatalogMongoDBAdaptor is ready when contains the METADATA_OBJECT
     * @return
     */
    @Override
    public boolean isCatalogDBReady() {
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_OBJECT_ID));
        return queryResult.getResult().get(0) == 1;
    }

    @Override
    public void close(){
        mongoManager.close(db.getDatabaseName());
    }


    /**
     Auxiliary query methods
     */
    protected int getNewId()  {return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);}

    /**
     * Study methods
     * ***************************
     */

    @Override
    public boolean studyExists(int studyId) {
        QueryResult<Long> count = studyCollection.count(new BasicDBObject(_ID, studyId));
        return count.getResult().get(0) != 0;
    }

    @Override
    public void checkStudyId(int studyId) throws CatalogDBException {
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
            String fileErrorMsg = createFile(study.getId(), file, options).getErrorMsg();
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
        if(!userDBAdaptor.projectExists(projectId)) {
            throw CatalogDBException.idNotFound("Project", projectId);
        }

        DBObject query = new BasicDBObject(_PROJECT_ID, projectId);

        QueryResult<DBObject> queryResult = studyCollection.find(query, filterOptions(options, FILTER_ROUTE_STUDIES));

        List<Study> studies = parseStudies(queryResult);
        for (Study study : studies) {
            joinFields(study.getId(), study, options);
        }
        return endQuery("Get all studies", startTime, studies);
    }

    @Override
    public QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        //TODO: Parse QueryOptions include/exclude
        DBObject query = new BasicDBObject(_ID, studyId);
        QueryResult result = studyCollection.find(query, filterOptions(options, FILTER_ROUTE_STUDIES));
//        QueryResult queryResult = endQuery("get study", startTime, result);

        List<Study> studies = parseStudies(result);
        if (studies.isEmpty()) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        joinFields(studyId, studies.get(0), options);

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

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap(("type"), Study.Type.class);
        filterEnumParams(parameters, studyParameters, acceptedEnums);

        if(parameters.containsKey("uri")) {
            URI uri = parameters.get("uri", URI.class);
            studyParameters.put("uri", uri.toString());
        }

        if(!studyParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject(_ID, studyId);
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
        DBObject query = new BasicDBObject(_ID, studyId);
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
        DBObject query = new BasicDBObject(_ID, studyId);
        DBObject projection = new BasicDBObject(_PROJECT_ID, "true");
        QueryResult<DBObject> result = studyCollection.find(query, projection, null);

        if (!result.getResult().isEmpty()) {
            DBObject study = result.getResult().get(0);
            Object id = study.get(_PROJECT_ID);
            return id instanceof Number? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("Study", studyId);
        }
    }

    @Override
    public String getStudyOwnerId(int studyId) throws CatalogDBException {
        int projectId = getProjectIdByStudyId(studyId);
        return userDBAdaptor.getProjectOwnerId(projectId);
    }

    @Override
    public QueryResult<Acl> getStudyAcl(int studyId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        BasicDBObject query = new BasicDBObject(_ID, studyId);
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
        if (!userDBAdaptor.userExists(userId)) {
            throw new CatalogDBException("Can not set ACL to non-existent user: " + userId);
        }

        DBObject newAclObject = getDbObject(newAcl, "ACL");

        BasicDBObject query = new BasicDBObject(_ID, studyId);
        BasicDBObject pull = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", newAcl.getUserId())));
        BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        studyCollection.update(query, pull, null);
        studyCollection.update(query, push, null);

        return getStudyAcl(studyId, userId);
    }

    @Override
    public QueryResult<Group> getGroup(int studyId, String userId, String groupId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        BasicDBObject query = new BasicDBObject(_ID, studyId);
        BasicDBObject groupQuery = new BasicDBObject();
        if (userId != null) {
            groupQuery.put("userIds", userId);
        }
        if (groupId != null) {
            groupQuery.put("id", groupId);
        }
        BasicDBObject project = new BasicDBObject("groups", new BasicDBObject("$elemMatch", groupQuery));

        QueryResult<DBObject> queryResult = studyCollection.find(query, project, options);
        List<Study> studies = CatalogMongoDBUtils.parseStudies(queryResult);
        List<Group> groups = new ArrayList<>(1);
        for (Study study : studies) {
            if (study.getGroups() != null) {
                groups.addAll(study.getGroups());
            }
        }

        return endQuery("getGroup", startTime, groups);
    }

    boolean groupExists(int studyId, String groupId) throws CatalogDBException{
        BasicDBObject query = new BasicDBObject(_ID, studyId).append("groups.id", groupId);
        return studyCollection.count(query).first() == 1;
    }

    @Override
    public QueryResult<Group> addMemberToGroup(int studyId, String groupId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        if (!groupExists(studyId, groupId)) {
            throw new CatalogDBException("Group \"" + groupId + "\" does not exists in study " + studyId);
        }

        BasicDBObject query = new BasicDBObject(_ID, studyId).append("groups.id", groupId);
        BasicDBObject update = new BasicDBObject("$addToSet", new BasicDBObject("groups.$.userIds", userId));

        QueryResult<WriteResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getN() != 1) {
            throw new CatalogDBException("Unable to add member to group " + groupId);
        }

        return endQuery("addMemberToGroup", startTime, getGroup(studyId, null, groupId, null));
    }

    @Override
    public QueryResult<Group> removeMemberFromGroup(int studyId, String groupId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        if (!groupExists(studyId, groupId)) {
            throw new CatalogDBException("Group \"" + groupId + "\" does not exists in study " + studyId);
        }

        BasicDBObject query = new BasicDBObject(_ID, studyId).append("groups.id", groupId);
        BasicDBObject update = new BasicDBObject("$pull", new BasicDBObject("groups.$.userIds", userId));

        QueryResult<WriteResult> queryResult = studyCollection.update(query, update, null);

        if (queryResult.first().getN() != 1) {
            throw new CatalogDBException("Unable to remove member to group " + groupId);
        }

        return endQuery("removeMemberFromGroup", startTime, getGroup(studyId, null, groupId, null));    }


    /**
     * File methods
     * ***************************
     */

    private boolean fileExists(int fileId) {
        QueryResult<Long> count = fileCollection.count(new BasicDBObject(_ID, fileId));
        return count.getResult().get(0) != 0;
    }

    private boolean filePathExists(int studyId, String path) {
        BasicDBObject query = new BasicDBObject(_STUDY_ID, studyId);
        query.put("path", path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    public void checkFileExists(int fileId) throws CatalogDBException {checkFileExists(fileId, "File");}

    public void checkFileExists(int fileId, String fileType) throws CatalogDBException {
        if (!fileExists(fileId)) {
            throw CatalogDBException.idNotFound(fileType, fileId);
        }
    }

    @Override
    public QueryResult<File> createFile(int studyId, File file, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        checkStudyId(studyId);
        String ownerId = getStudyOwnerId(studyId);

        if(filePathExists(studyId, file.getPath())){
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
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
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        return endQuery("Create file", startTime, getFile(newFileId, options));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException {
        long startTime = startQuery();

        WriteResult id = fileCollection.remove(new BasicDBObject(_ID, fileId), null).getResult().get(0);
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
    public QueryResult<File> getAllFilesInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> queryResult = fileCollection.find( new BasicDBObject(_STUDY_ID, studyId), filterOptions(options, FILTER_ROUTE_FILES));
        List<File> files = parseFiles(queryResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> folderResult = fileCollection.find(new BasicDBObject(_ID, folderId), filterOptions(options, FILTER_ROUTE_FILES));

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

        QueryResult<DBObject> queryResult = fileCollection.find(new BasicDBObject(_ID, fileId), filterOptions);

        File file = parseFile(queryResult);
        if(file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {"description", "uri", "creationDate", "modificationDate"};
        filterStringParams(parameters, fileParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = new HashMap<>();
        acceptedEnums.put("type", File.Type.class);
        acceptedEnums.put("format", File.Format.class);
        acceptedEnums.put("bioformat", File.Bioformat.class);
        acceptedEnums.put("status", File.Status.class);
        filterEnumParams(parameters, fileParameters, acceptedEnums);

        String[] acceptedLongParams = {"diskUsage"};
        filterLongParams(parameters, fileParameters, acceptedLongParams);

        String[] acceptedIntParams = {"jobId"};
        filterIntParams(parameters, fileParameters, acceptedIntParams);
        if (parameters.containsKey("jobId")) {
            if (!jobExists(parameters.getInt("jobId"))) {
                throw CatalogDBException.idNotFound("Job", parameters.getInt("jobId"));
            }
        }

        String[] acceptedIntegerListParams = {"sampleIds"};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);
        if (parameters.containsKey("sampleIds")) {
            for (Integer sampleId : parameters.getAsIntegerList("sampleIds")) {
                if (!sampleDBAdaptor.sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, fileParameters, acceptedMapParams);

        String[] acceptedObjectParams = {"index"};
        filterObjectParams(parameters, fileParameters, acceptedObjectParams);

        if(!fileParameters.isEmpty()) {
            QueryResult<WriteResult> update = fileCollection.update(new BasicDBObject(_ID, fileId),
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
                String replacedPath = subFile.getPath().replaceFirst(oldPath, filePath);
                renameFile(subFile.getId(), replacedPath); // first part of the path in the subfiles 3
            }
        }
        BasicDBObject query = new BasicDBObject(_ID, fileId);
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
        DBObject query = new BasicDBObject(_ID, fileId);
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
                .append(_ID, false)
                .get();

        QueryResult queryResult = fileCollection.find(new BasicDBObject(_ID, fileId), projection, null);
        if (queryResult.getNumResults() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        List<Acl> acl = parseFile(queryResult).getAcl();
        return endQuery("get file acl", startTime, acl);
    }

    @Override
    public QueryResult<Map<String, Map<String, Acl>>> getFilesAcl(int studyId, List<String> filePaths, List<String> userIds) throws CatalogDBException {

        long startTime = startQuery();
        DBObject match = new BasicDBObject("$match", new BasicDBObject(_STUDY_ID, studyId).append("path", new BasicDBObject("$in", filePaths)));
        DBObject unwind = new BasicDBObject("$unwind", "$acl");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
        DBObject project = new BasicDBObject("$project", new BasicDBObject("path", 1).append("id", 1).append("acl", 1));

        QueryResult<DBObject> aggregate = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        List<File> files = parseFiles(aggregate);
        Map<String, Map<String, Acl>> pathAclMap = new HashMap<>();
        for (File file : files) {
            Acl acl = file.getAcl().get(0);
            if (pathAclMap.containsKey(file.getPath())) {
                pathAclMap.get(file.getPath()).put(acl.getUserId(), acl);
            } else {
                HashMap<String, Acl> map = new HashMap<>();
                map.put(acl.getUserId(), acl);
                pathAclMap.put(file.getPath(), map);
            }
        }
//        Map<String, Acl> pathAclMap = files.stream().collect(Collectors.toMap(File::getPath, file -> file.getAcl().get(0)));

        return endQuery("getFilesAcl", startTime, Collections.singletonList(pathAclMap));
    }

    @Override
    public QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();

        this.getCatalogUserDBAdaptor().checkUserExists(userId);

        DBObject newAclObject = getDbObject(newAcl, "ACL");

        List<Acl> aclList = getFileAcl(fileId, userId).getResult();
        DBObject match;
        DBObject updateOperation;
        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
            match = new BasicDBObject(_ID, fileId);
            updateOperation = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            match = BasicDBObjectBuilder
                    .start(_ID, fileId)
                    .append("acl.userId", userId).get();
            updateOperation = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        QueryResult update = fileCollection.update(match, updateOperation, null);
        return endQuery("set file acl", startTime);
    }

    public QueryResult<File> getAllFiles(QueryOptions query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                FileFilterOption option = FileFilterOption.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option, option.name(), query, _ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option, option.name(), query, _STUDY_ID, mongoQueryList);
                        break;
                    case directory:
                        mongoQueryList.add(new BasicDBObject("path", new BasicDBObject("$regex", "^" + query.getString("directory") + "[^/]+/?$")));
                        break;
                    default:
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), query, queryKey, mongoQueryList);
                        break;
                    case minSize:
                        mongoQueryList.add(new BasicDBObject("size", new BasicDBObject("$gt", query.getInt("minSize"))));
                        break;
                    case maxSize:
                        mongoQueryList.add(new BasicDBObject("size", new BasicDBObject("$lt", query.getInt("maxSize"))));
                        break;
                    case like:
                        mongoQueryList.add(new BasicDBObject("name", new BasicDBObject("$regex", query.getString("like"))));
                        break;
                    case startsWith:
                        mongoQueryList.add(new BasicDBObject("name", new BasicDBObject("$regex", "^" + query.getString("startsWith"))));
                        break;
                    case startDate:
                        mongoQueryList.add(new BasicDBObject("creationDate", new BasicDBObject("$lt", query.getString("startDate"))));
                        break;
                    case endDate:
                        mongoQueryList.add(new BasicDBObject("creationDate", new BasicDBObject("$gt", query.getString("endDate"))));
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        BasicDBObject andQuery = new BasicDBObject("$and", mongoQueryList);
        logger.debug("File search query : {}", andQuery);
        QueryResult<DBObject> queryResult = fileCollection.find(andQuery, filterOptions(options, FILTER_ROUTE_FILES));

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
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        }
    }

    /**
     * Job methods
     * ***************************
     */

    @Override
    public boolean jobExists(int jobId) {
        QueryResult<Long> count = jobCollection.count(new BasicDBObject(_ID, jobId));
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
        WriteResult id = jobCollection.remove(new BasicDBObject(_ID, jobId), null).getResult().get(0);
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
        QueryResult<DBObject> queryResult = jobCollection.find(new BasicDBObject(_ID, jobId), filterOptions(options, FILTER_ROUTE_JOBS));
        Job job = parseJob(queryResult);
        if(job != null) {
            return endQuery("Get job", startTime, Arrays.asList(job));
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> getAllJobsInStudy(int studyId, QueryOptions options) throws CatalogDBException {
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

        BasicDBObject query = new BasicDBObject(_ID, jobId);
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
                checkFileExists(fileId, "Output File");
            }
        }

        String[] acceptedMapParams = {"attributes", "resourceManagerAttributes"};
        filterMapParams(parameters, jobParameters, acceptedMapParams);

        if(!jobParameters.isEmpty()) {
            BasicDBObject query = new BasicDBObject(_ID, jobId);
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
        DBObject query = new BasicDBObject(_ID, jobId);
        DBObject projection = new BasicDBObject(_STUDY_ID, true);
        QueryResult<DBObject> queryResult = jobCollection.find(query, projection, null);

        if (queryResult.getNumResults() != 0) {
            Object id = queryResult.getResult().get(0).get(_STUDY_ID);
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        } else {
            throw CatalogDBException.idNotFound("Job", jobId);
        }
    }

    @Override
    public QueryResult<Job> getAllJobs(QueryOptions query, QueryOptions options) throws CatalogDBException {
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

        if (!userDBAdaptor.userExists(userId)) {
            throw new CatalogDBException("User {id:" + userId + "} does not exist");
        }

        // Check if tools.alias already exists.
        DBObject countQuery = BasicDBObjectBuilder
                .start(_ID, userId)
                .append("tools.alias", tool.getAlias())
                .get();
        QueryResult<Long> count = userCollection.count(countQuery);
        if(count.getResult().get(0) != 0){
            throw new CatalogDBException( "Tool {alias:\"" + tool.getAlias() + "\"} already exists in this user");
        }

        tool.setId(getNewId());

        DBObject toolObject = getDbObject(tool, "tool");
        DBObject query = new BasicDBObject(_ID, userId);
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
                .start(_ID, userId)
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

    private void joinFields(int studyId, Study study, QueryOptions options) throws CatalogDBException {
        if (studyId <= 0) {
            return;
        }

        if (options == null) {
            study.setDiskUsage(getDiskUsageByStudy(studyId));
            return;
        }

        List<String> include = options.getAsStringList("include");
        List<String> exclude = options.getAsStringList("exclude");
        if((!include.isEmpty() && include.contains(FILTER_ROUTE_STUDIES + "diskUsage")) ||
                (!exclude.isEmpty() && !exclude.contains(FILTER_ROUTE_STUDIES + "diskUsage"))) {
            study.setDiskUsage(getDiskUsageByStudy(studyId));
        }

        if (options.getBoolean("includeFiles")) {
            study.setFiles(getAllFilesInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeJobs")) {
            study.setJobs(getAllJobsInStudy(studyId, options).getResult());
        }
        if (options.getBoolean("includeSamples")) {
            study.setSamples(sampleDBAdaptor.getAllSamples(new QueryOptions(CatalogSampleDBAdaptor.SampleFilterOption.studyId.toString(), studyId)).getResult());
        }
    }

}
