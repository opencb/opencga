package org.opencb.opencga.catalog.db.mongodb2;

import com.mongodb.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api2.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb2.CatalogMongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoFileDBAdaptor extends CatalogMongoDBAdaptor implements CatalogFileDBAdaptor {

    private final CatalogMongoDBAdaptorFactory dbAdaptorFactory;
    private final MongoDBCollection fileCollection;

    public CatalogMongoFileDBAdaptor(CatalogMongoDBAdaptorFactory dbAdaptorFactory, MongoDBCollection fileCollection) {
        super(LoggerFactory.getLogger(CatalogMongoFileDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
    }

    private boolean filePathExists(int studyId, String path) {
        BasicDBObject query = new BasicDBObject(_STUDY_ID, studyId);
        query.put("path", path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    public void checkFileExists(int fileId) throws CatalogDBException {
        checkFileExists(fileId, "File");
    }

    public void checkFileExists(int fileId, String fileType) throws CatalogDBException {
        if (!fileExists(fileId)) {
            throw CatalogDBException.idNotFound(fileType, fileId);
        }
    }

    @Override
    public QueryResult<File> createFile(int studyId, File file, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        getCatalogStudyDBAdaptor().checkStudyId(studyId);
        String ownerId = getCatalogStudyDBAdaptor().getStudyOwnerId(studyId);

        if (filePathExists(studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        //new File Id
        int newFileId = getNewId();
        file.setId(newFileId);
        if (file.getOwnerId() == null) {
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
        if (id.getN() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        } else {
            deletes.add(id.getN());
            return endQuery("delete file", startTime, deletes);
        }
    }

    @Override
    public int getFileId(int studyId, String path) throws CatalogDBException {
        DBObject query = BasicDBObjectBuilder.start(_STUDY_ID, studyId).append("path", path).get();
        BasicDBObject projection = new BasicDBObject("id", true);
        QueryResult<DBObject> queryResult = fileCollection.find(query, projection, null);
        File file = parseFile(queryResult);
        return file != null ? file.getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFilesInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> queryResult = fileCollection.find(new BasicDBObject(_STUDY_ID, studyId), filterOptions(options,
                FILTER_ROUTE_FILES));
        List<File> files = parseFiles(queryResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<DBObject> folderResult = fileCollection.find(new BasicDBObject(_ID, folderId), filterOptions(options,
                FILTER_ROUTE_FILES));

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
        if (file != null) {
            return endQuery("Get file", startTime, Arrays.asList(file));
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public QueryResult<File> modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
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

        if (!fileParameters.isEmpty()) {
            QueryResult<WriteResult> update = fileCollection.update(new BasicDBObject(_ID, fileId),
                    new BasicDBObject("$set", fileParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
                throw CatalogDBException.idNotFound("File", fileId);
            }
        }

        return endQuery("Modify file", startTime, getFile(fileId, null));
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
            filePath += filePath.endsWith("/") ? "" : "/";
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
        if (fileQueryResult == null || fileQueryResult.getResult() == null || fileQueryResult.getResult().isEmpty()) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return fileQueryResult.getResult().get(0).getOwnerId();
//        int studyId = getStudyIdByFileId(fileId);
//        return getStudyOwnerId(studyId);
    }

    /**
     * query: db.file.find({id:2}, {acl:{$elemMatch:{userId:"jcoll"}}, studyId:1})
     */
    @Override
    public QueryResult<AclEntry> getFileAcl(int fileId, String userId) throws CatalogDBException {
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
        List<AclEntry> acl = parseFile(queryResult).getAcl();
        return endQuery("get file acl", startTime, acl);
    }

    @Override
    public QueryResult<Map<String, Map<String, AclEntry>>> getFilesAcl(int studyId, List<String> filePaths, List<String> userIds) throws
            CatalogDBException {

        long startTime = startQuery();
        DBObject match = new BasicDBObject("$match", new BasicDBObject(_STUDY_ID, studyId).append("path", new BasicDBObject("$in",
                filePaths)));
        DBObject unwind = new BasicDBObject("$unwind", "$acl");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
        DBObject project = new BasicDBObject("$project", new BasicDBObject("path", 1).append("id", 1).append("acl", 1));

        QueryResult<DBObject> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

//        QueryResult<DBObject> result = fileCollection.find(new BasicDBObject("path", new BasicDBObject("$in", filePaths))
//                .append(_STUDY_ID, studyId), new BasicDBObject("acl", true), null);

        List<File> files = parseFiles(result);
        Map<String, Map<String, AclEntry>> pathAclMap = new HashMap<>();
        for (File file : files) {
//            AclEntry acl = file.getAcl().get(0);
            for (AclEntry acl : file.getAcl()) {
                if (pathAclMap.containsKey(file.getPath())) {
                    pathAclMap.get(file.getPath()).put(acl.getUserId(), acl);
                } else {
                    HashMap<String, AclEntry> map = new HashMap<>();
                    map.put(acl.getUserId(), acl);
                    pathAclMap.put(file.getPath(), map);
                }
            }
        }
//        Map<String, Acl> pathAclMap = files.stream().collect(Collectors.toMap(File::getPath, file -> file.getAcl().get(0)));
        logger.debug("getFilesAcl for {} paths and {} users, dbTime: {} ", filePaths.size(), userIds.size(), result.getDbTime());
        return endQuery("getFilesAcl", startTime, Collections.singletonList(pathAclMap));
    }

    @Override
    public QueryResult<AclEntry> setFileAcl(int fileId, AclEntry newAcl) throws CatalogDBException {
        long startTime = startQuery();
        String userId = newAcl.getUserId();

        checkAclUserId(this, userId, getStudyIdByFileId(fileId));

        DBObject newAclObject = getDbObject(newAcl, "ACL");

        List<AclEntry> aclList = getFileAcl(fileId, userId).getResult();
        DBObject query;
        DBObject update;
        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
            query = new BasicDBObject(_ID, fileId);
            update = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
        } else {    // there is already another ACL: overwrite
            query = BasicDBObjectBuilder
                    .start(_ID, fileId)
                    .append("acl.userId", userId).get();
            update = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
        }
        QueryResult<WriteResult> queryResult = fileCollection.update(query, update, null);
        if (queryResult.first().getN() != 1) {
            throw CatalogDBException.idNotFound("File", fileId);
        }

        return endQuery("setFileAcl", startTime, getFileAcl(fileId, userId));
    }

    @Override
    public QueryResult<AclEntry> unsetFileAcl(int fileId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<AclEntry> fileAcl = getFileAcl(fileId, userId);
        DBObject query = new BasicDBObject(_ID, fileId);
        ;
        DBObject update = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", userId)));

        QueryResult queryResult = fileCollection.update(query, update, null);

        return endQuery("unsetFileAcl", startTime, fileAcl);
    }

    @Override
    public int getStudyIdByDatasetId(int datasetId) throws CatalogDBException {
        BasicDBObject query = new BasicDBObject("datasets.id", datasetId);
        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", 1), null);
        if (queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsField("id")) {
            throw CatalogDBException.idNotFound("Dataset", datasetId);
        } else {
            Object id = queryResult.getResult().get(0).get("id");
            return id instanceof Number ? ((Number) id).intValue() : (int) Double.parseDouble(id.toString());
        }
    }

//    public QueryResult<File> getAllFiles(Query query, QueryOptions options) throws CatalogDBException {
//        long startTime = startQuery();
//
//        List<DBObject> mongoQueryList = new LinkedList<>();
//
//        for (Map.Entry<String, Object> entry : query.entrySet()) {
//            String key = entry.getKey().split("\\.")[0];
//            try {
//                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
//                    continue;   //Exclude DataStore options
//                }
//                FileFilterOption option = FileFilterOption.valueOf(key);
//                switch (option) {
//                    case id:
//                        addCompQueryFilter(option, option.name(), query, _ID, mongoQueryList);
//                        break;
//                    case studyId:
//                        addCompQueryFilter(option, option.name(), query, _STUDY_ID, mongoQueryList);
//                        break;
//                    case directory:
//                        mongoQueryList.add(new BasicDBObject("path", new BasicDBObject("$regex", "^" + query.getString("directory") +
//                                "[^/]+/?$")));
//                        break;
//                    default:
//                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
//                        addCompQueryFilter(option, entry.getKey(), query, queryKey, mongoQueryList);
//                        break;
//                    case minSize:
//                        mongoQueryList.add(new BasicDBObject("size", new BasicDBObject("$gt", query.getInt("minSize"))));
//                        break;
//                    case maxSize:
//                        mongoQueryList.add(new BasicDBObject("size", new BasicDBObject("$lt", query.getInt("maxSize"))));
//                        break;
//                    case like:
//                        mongoQueryList.add(new BasicDBObject("name", new BasicDBObject("$regex", query.getString("like"))));
//                        break;
//                    case startsWith:
//                        mongoQueryList.add(new BasicDBObject("name", new BasicDBObject("$regex", "^" + query.getString("startsWith"))));
//                        break;
//                    case startDate:
//                        mongoQueryList.add(new BasicDBObject("creationDate", new BasicDBObject("$lt", query.getString("startDate"))));
//                        break;
//                    case endDate:
//                        mongoQueryList.add(new BasicDBObject("creationDate", new BasicDBObject("$gt", query.getString("endDate"))));
//                        break;
//                }
//            } catch (IllegalArgumentException e) {
//                throw new CatalogDBException(e);
//            }
//        }
//
//        BasicDBObject mongoQuery = new BasicDBObject("$and", mongoQueryList);
//        QueryOptions queryOptions = filterOptions(options, FILTER_ROUTE_FILES);
////        QueryResult<DBObject> queryResult = fileCollection.find(mongoQuery, null, File.class, queryOptions);
//        QueryResult<File> queryResult = fileCollection.find(mongoQuery, null, new ComplexTypeConverter<File, DBObject>() {
//            @Override
//            public File convertToDataModelType(DBObject object) {
//                try {
//                    return getObjectReader(File.class).readValue(restoreDotsInKeys(object).toString());
//                } catch (IOException e) {
//                    return null;
//                }
//            }
//
//            @Override
//            public DBObject convertToStorageType(File object) {
//                return null;
//            }
//        }, queryOptions);
//        logger.debug("File search: query : {}, project: {}, dbTime: {}", mongoQuery, queryOptions == null ? "" : queryOptions.toJson(),
//                queryResult.getDbTime());
////        List<File> files = parseFiles(queryResult);
//
//        return endQuery("Search File", startTime, queryResult);
//    }

    @Override
    public QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        getCatalogStudyDBAdaptor().checkStudyId(studyId);

        QueryResult<Long> count = studyCollection.count(BasicDBObjectBuilder
                .start(_ID, studyId)
                .append("datasets.name", dataset.getName())
                .get());

        if (count.getResult().get(0) > 0) {
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
        if (studies == null || studies.get(0).getDatasets().isEmpty()) {
            throw CatalogDBException.idNotFound("Dataset", datasetId);
        } else {
            return endQuery("readDataset", startTime, studies.get(0).getDatasets());
        }
    }


    @Override
    public QueryResult<Long> count(Query query) {
        return null;
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<File> get(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult<File> update(Query query, ObjectMap parameters) {
        return null;
    }

    @Override
    public QueryResult<Long> delete(Query query) {
        return null;
    }

    @Override
    public Iterator<File> iterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public Iterator nativeIterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {

    }

    private Bson parseQuery(Query query) {
        List<Bson> andBsonList = new ArrayList<>();

        // FIXME: Pedro. Check the mongodb names as well as integer createQueries

        createOrQuery(query, QueryParams.ID.key(), "id", andBsonList);
        createOrQuery(query, QueryParams.NAME.key(), "name", andBsonList);
        createOrQuery(query, QueryParams.TYPE.key(), "type", andBsonList);
        createOrQuery(query, QueryParams.FORMAT.key(), "format", andBsonList);
        createOrQuery(query, QueryParams.BIOFORMAT.key(), "bioformat", andBsonList);
        createOrQuery(query, QueryParams.DELETE_DATE.key(), "deleteDate", andBsonList);
        createOrQuery(query, QueryParams.OWNER_ID.key(), "ownerId", andBsonList);
        createOrQuery(query, QueryParams.CREATION_DATE.key(), "creationDate", andBsonList);
        createOrQuery(query, QueryParams.MODIFICATION_DATE.key(), "modificationDate", andBsonList);
        createOrQuery(query, QueryParams.STATUS.key(), "status", andBsonList);
        createOrQuery(query, QueryParams.DISK_USAGE.key(), "diskUsage", andBsonList);
        createOrQuery(query, QueryParams.EXPERIMENT_ID.key(), "experimentId", andBsonList);
        createOrQuery(query, QueryParams.JOB_ID.key(), "jobId", andBsonList);
        createOrQuery(query, QueryParams.SAMPLE_ID.key(), "sampleId", andBsonList);

        createOrQuery(query, QueryParams.ACL_USER_ID.key(), "acl.userId", andBsonList);
        createOrQuery(query, QueryParams.ACL_READ.key(), "acl.read", andBsonList);
        createOrQuery(query, QueryParams.ACL_WRITE.key(), "acl.write", andBsonList);
        createOrQuery(query, QueryParams.ACL_EXECUTE.key(), "acl.execute", andBsonList);
        createOrQuery(query, QueryParams.ACL_DELETE.key(), "acl.delete", andBsonList);

        createOrQuery(query, QueryParams.STUDY_ID.key(), "study.id", andBsonList);

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
