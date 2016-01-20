package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.*;
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
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
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

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoFileDBAdaptor extends CatalogMongoDBAdaptor implements CatalogFileDBAdaptor {

    //private final CatalogMongoDBAdaptorFactory dbAdaptorFactory;
    private final MongoDBCollection fileCollection;
    private FileConverter fileConverter;

    public CatalogMongoFileDBAdaptor(MongoDBCollection fileCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoFileDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.fileConverter = new FileConverter();
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

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        String ownerId = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyOwnerId(studyId);

        if (filePathExists(studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        //new File Id
        int newFileId = getNewId();
        file.setId(newFileId);
        if (file.getOwnerId() == null) {
            file.setOwnerId(ownerId);
        }
        Document fileDBObject = getMongoDBDocument(file, "File");
        fileDBObject.put(_STUDY_ID, studyId);
        fileDBObject.put(_ID, newFileId);

        try {
            fileCollection.insert(fileDBObject, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        // Update the diskUsage field from the study collection
        try {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getDiskUsage());
        } catch (CatalogDBException e) {
            deleteFile(newFileId);
            throw new CatalogDBException("File from study { id:" + studyId + "} was removed from the database due to problems " +
                    "with the study collection.");
        }

        return endQuery("Create file", startTime, getFile(newFileId, options));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Override
    public QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException {
        long startTime = startQuery();

//        WriteResult id = fileCollection.remove(new BasicDBObject(_ID, fileId), null).getResult().get(0);
        DeleteResult deleteResult = fileCollection.remove(new BasicDBObject(_ID, fileId), null).getResult().get(0);
        List<Integer> deletes = new LinkedList<>();
        if (deleteResult.getDeletedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        } else {
            deletes.add((int) deleteResult.getDeletedCount());
            return endQuery("delete file", startTime, deletes);
        }
    }

    @Override
    public int getFileId(int studyId, String path) throws CatalogDBException {
//        DBObject query = BasicDBObjectBuilder.start(_STUDY_ID, studyId).append("path", path).get();
//        BasicDBObject projection = new BasicDBObject("id", true);
//        QueryResult<DBObject> queryResult = fileCollection.find(query, projection, null);
//        File file = parseFile(queryResult);

        Bson query = Filters.and(Filters.eq(_STUDY_ID, studyId), Filters.eq("path", path));
        Bson projection = Projections.include("id");
        QueryResult<Document> queryResult = fileCollection.find(query, projection, new QueryOptions());
        File file = parseFile(queryResult);
        return file != null ? file.getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFilesInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(_STUDY_ID, studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<Document> folderResult = fileCollection.find(Filters.eq(_ID, folderId), filterOptions(options,
                FILTER_ROUTE_FILES));

        File folder = parseFile(folderResult);
        if (!folder.getType().equals(File.Type.FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Object studyId = folderResult.getResult().get(0).get(_STUDY_ID);

//        BasicDBObject query = new BasicDBObject(_STUDY_ID, studyId);
//        query.put("path", new BasicDBObject("$regex", "^" + folder.getPath() + "[^/]+/?$"));
        Bson query = Filters.and(Filters.eq(_STUDY_ID, studyId), Filters.regex("path", "^" + folder.getPath() + "[^/]+/?$"));
//        QueryResult<DBObject> filesResult = fileCollection.find(query, null);
        QueryResult<Document> filesResult = fileCollection.find(query, null);
        List<File> files = parseFiles(filesResult);

        return endQuery("Get all files", startTime, files);
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        QueryOptions filterOptions = filterOptions(options, FILTER_ROUTE_FILES);

        QueryResult<Document> queryResult = fileCollection.find(Filters.eq(_ID, fileId), filterOptions);
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
//            if (!jobExists(parameters.getInt("jobId"))) {
            if (!this.dbAdaptorFactory.getCatalogJobDBAdaptor().jobExists(parameters.getInt("jobId"))) {
                throw CatalogDBException.idNotFound("Job", parameters.getInt("jobId"));
            }
        }

        String[] acceptedIntegerListParams = {"sampleIds"};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);
        if (parameters.containsKey("sampleIds")) {
            for (Integer sampleId : parameters.getAsIntegerList("sampleIds")) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {"attributes", "stats"};
        filterMapParams(parameters, fileParameters, acceptedMapParams);

        String[] acceptedObjectParams = {"index"};
        filterObjectParams(parameters, fileParameters, acceptedObjectParams);

        if (!fileParameters.isEmpty()) {
//            QueryResult<WriteResult> update = fileCollection.update(Filters.eq(_ID, fileId),
//                    new BasicDBObject("$set", fileParameters), null);
            QueryResult<UpdateResult> update = fileCollection.update(Filters.eq(_ID, fileId), new Document("$set", fileParameters), null);
            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
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
        QueryResult<UpdateResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return endQuery("rename file", startTime);
    }

    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
//        DBObject query = new BasicDBObject(_ID, fileId);
//        DBObject projection = new BasicDBObject(_STUDY_ID, "true");
        QueryResult<Document> result = fileCollection.find(Filters.eq(_ID, fileId), Projections.include(_STUDY_ID), null);

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
//        DBObject projection = BasicDBObjectBuilder
//                .start("acl",
//                        new BasicDBObject("$elemMatch",
//                                new BasicDBObject("userId", userId)))
//                .append(_ID, false)
//                .get();

        Bson projection = Projections.elemMatch("acl", Filters.eq("userId", userId));
        QueryResult queryResult = fileCollection.find(Filters.eq(_ID, fileId), projection, null);
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
//        DBObject match = new BasicDBObject("$match", new BasicDBObject(_STUDY_ID, studyId).append("path", new BasicDBObject("$in",
//                filePaths)));
//        DBObject unwind = new BasicDBObject("$unwind", "$acl");
//        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
//        DBObject project = new BasicDBObject("$project", new BasicDBObject("path", 1).append("id", 1).append("acl", 1));
//        QueryResult<DBObject> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        Bson match = Aggregates.match(Filters.and(Filters.eq(_STUDY_ID, studyId), Filters.in("acl.userId", userIds)));
        Bson unwind = Aggregates.unwind("$acl");
        Bson match2 = Aggregates.match(Filters.and(Filters.in("acl.userId", userIds)));
        Bson project = Aggregates.project(Projections.include("id", "path", "acl"));
        QueryResult<Document> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

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

        checkAclUserId(dbAdaptorFactory, userId, getStudyIdByFileId(fileId));

        //DBObject newAclObject = getDbObject(newAcl, "ACL");
        Document newAclObject = getMongoDBDocument(newAcl, "ACL");

        List<AclEntry> aclList = getFileAcl(fileId, userId).getResult();
        Bson query;
        Bson update;
        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
            //query = new BasicDBObject(_ID, fileId);
            // update = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
            query = Filters.eq(_ID, fileId);
            update = Updates.push("acl", newAclObject);
        } else {    // there is already another ACL: overwrite
            /*query = BasicDBObjectBuilder
                    .start(_ID, fileId)
                    .append("acl.userId", userId).get();
            update = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));*/
            query = Filters.and(Filters.eq(_ID, fileId), Filters.eq("acl.userId", userId));
            update = Updates.set("acl.$", newAclObject);
        }
        QueryResult<UpdateResult> queryResult = fileCollection.update(query, update, null);
        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.idNotFound("File", fileId);
        }

        return endQuery("setFileAcl", startTime, getFileAcl(fileId, userId));
    }

    @Override
    public QueryResult<AclEntry> unsetFileAcl(int fileId, String userId) throws CatalogDBException {
        long startTime = startQuery();

        QueryResult<AclEntry> fileAcl = getFileAcl(fileId, userId);
//        DBObject query = new BasicDBObject(_ID, fileId);
//        DBObject update = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", userId)));
//        QueryResult queryResult = fileCollection.update(query, update, null);

        Bson query = Filters.eq(_ID, fileId);
        Bson update = Updates.pull("acl", new Document("userId", userId));
        QueryResult<UpdateResult> queryResult = fileCollection.update(query, update, null);

        return endQuery("unsetFileAcl", startTime, fileAcl);
    }

    @Override
    public int getStudyIdByDatasetId(int datasetId) throws CatalogDBException {
        BasicDBObject query = new BasicDBObject("datasets.id", datasetId);
//        QueryResult<DBObject> queryResult = studyCollection.find(query, new BasicDBObject("id", 1), null);
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor()
                .nativeGet(new Query("datasets.id", datasetId), new QueryOptions("include", "id"));
        if (queryResult.getResult().isEmpty() || !queryResult.getResult().get(0).containsKey("id")) {
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
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);

//        QueryResult<Long> count = studyCollection.count(BasicDBObjectBuilder
//                .start(_ID, studyId)
//                .append("datasets.name", dataset.getName())
//                .get());

        Query query = new Query(_ID, studyId)
                .append("datasets.name", dataset.getName());
        QueryResult<Long> count = dbAdaptorFactory.getCatalogStudyDBAdaptor().count(query);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Dataset { name: \"" + dataset.getName() + "\" } already exists in this study.");
        }

        int newId = getNewId();
        dataset.setId(newId);

        Document datasetObject = getMongoDBDocument(dataset, "Dataset");
        QueryResult<UpdateResult> update = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .update(Filters.eq(_ID, studyId), new BasicDBObject("$push", new BasicDBObject("datasets", datasetObject)), null);

        if (update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("Study", studyId);
        }

        return endQuery("createDataset", startTime, getDataset(newId, options));
    }

    @Override
    public QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

//        BasicDBObject query = new BasicDBObject("datasets.id", datasetId);
//        BasicDBObject projection = new BasicDBObject("datasets", new BasicDBObject("$elemMatch", new BasicDBObject("id", datasetId)));
//        QueryResult<DBObject> queryResult = studyCollection.find(query, projection, filterOptions(options, FILTER_ROUTE_STUDIES));

        Bson query = Filters.eq("datasets.id", datasetId);
        Bson projection = Projections.elemMatch("datasets", Filters.eq("id", datasetId));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .find(query, projection, filterOptions(options, FILTER_ROUTE_STUDIES));

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
        Bson bson = parseQuery(query);
        options = filterOptions(options, FILTER_ROUTE_FILES);
        return fileCollection.find(bson, Projections.exclude(_ID, _STUDY_ID), fileConverter, options);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult<File> update(int id, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {

        long startTime = startQuery();

        // If the user wants to change the diskUsages of the file(s), we first make a query to obtain the old values.
        QueryResult fileQueryResult = null;
        if (parameters.containsKey(QueryParams.DISK_USAGE.key())) {
            QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(QueryParams.DISK_USAGE.key(),
                    QueryParams.STUDY_ID.key()));
            fileQueryResult = nativeGet(query, queryOptions);
        }

        // We perform the update.
        Bson queryBson = parseQuery(query);
        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(),
                QueryParams.MODIFICATION_DATE.key()};
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, fileParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = new HashMap<>();
        acceptedEnums.put("type", File.Type.class);
        acceptedEnums.put("format", File.Format.class);
        acceptedEnums.put("bioformat", File.Bioformat.class);
        acceptedEnums.put("status", File.Status.class);
        try {
            filterEnumParams(parameters, fileParameters, acceptedEnums);
        } catch (CatalogDBException e) {
            e.printStackTrace();
            throw new CatalogDBException("File update: It was impossible updating the files. " + e.getMessage());
        }

        String[] acceptedLongParams = {QueryParams.DISK_USAGE.key()};
        filterLongParams(parameters, fileParameters, acceptedLongParams);

        String[] acceptedIntParams = {QueryParams.JOB_ID.key()};
        // Fixme: Add "experiment_id" ?
        filterIntParams(parameters, fileParameters, acceptedIntParams);
        // Check if the job exists.
        if (parameters.containsKey(QueryParams.JOB_ID.key())) {
            if (!this.dbAdaptorFactory.getCatalogJobDBAdaptor().jobExists(parameters.getInt(QueryParams.JOB_ID.key()))) {
                throw CatalogDBException.idNotFound("Job", parameters.getInt(QueryParams.JOB_ID.key()));
            }
        }

        String[] acceptedIntegerListParams = {QueryParams.SAMPLE_IDS.key()};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);
        // Check if the sample ids exist.
        if (parameters.containsKey(QueryParams.SAMPLE_IDS.key())) {
            for (Integer sampleId : parameters.getAsIntegerList(QueryParams.SAMPLE_IDS.key())) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, fileParameters, acceptedMapParams);
        // Fixme: Attributes and stats can be also parsed to numeric or boolean

        String[] acceptedObjectParams = {"index"};
        filterObjectParams(parameters, fileParameters, acceptedObjectParams);

        if (!fileParameters.isEmpty()) {
            QueryResult<UpdateResult> update = fileCollection.update(queryBson, new Document("$set", fileParameters), null);
//            if (update.getResult().isEmpty() || update.getNumTotalResults() == 0) {
//                throw new CatalogDBException("File update: Could not update the file(s).");
//            }

            // If the diskUsage of some of the files have been changed, notify to the correspondent study
            if (fileQueryResult != null) {
                long newDiskUsage = parameters.getLong(QueryParams.DISK_USAGE.key());
                for (Document file : (ArrayList<Document>) fileQueryResult.getResult()) {
                    long difDiskUsage = newDiskUsage - (long) file.get(QueryParams.DISK_USAGE.key());
                    int studyId = (int) file.get(_STUDY_ID);
                    try {
                        dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, difDiskUsage);
                    } catch (CatalogDBException e) {
                        // Fixme: What do we do if it cannot be notified if everything has been updated already?
                        // Could we remove all the files from the database and insert them again?
                    }
                }
            }
            return endQuery("Modify file", startTime, Collections.singletonList(update.getNumTotalResults()));
        }

//        return endQuery("Modify file", startTime, get(query, null));
        return endQuery("Modify file", startTime, Collections.singletonList(0L));
    }

    @Override
    public QueryResult<File> delete(int id) throws CatalogDBException {
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

        addIntegerOrQuery("id", QueryParams.ID.key(), query, andBsonList);
        addStringOrQuery("name", QueryParams.NAME.key(), query, andBsonList);
        addStringOrQuery("type", QueryParams.TYPE.key(), query, andBsonList);
        addStringOrQuery("format", QueryParams.FORMAT.key(), query, andBsonList);
        addStringOrQuery("bioformat", QueryParams.BIOFORMAT.key(), query, andBsonList);
        addStringOrQuery("uri", QueryParams.URI.key(), query, andBsonList);
        addStringOrQuery("deleteDate", QueryParams.DELETE_DATE.key(), query, andBsonList);
        addStringOrQuery("ownerId", QueryParams.OWNER_ID.key(), query, andBsonList);
        addStringOrQuery("creationDate", QueryParams.CREATION_DATE.key(), query, andBsonList);
        addStringOrQuery("modificationDate", QueryParams.MODIFICATION_DATE.key(), query, andBsonList);
        addStringOrQuery("description", QueryParams.DESCRIPTION.key(), query, andBsonList);
        addStringOrQuery("status", QueryParams.STATUS.key(), query, andBsonList);
        addStringOrQuery("diskUsage", QueryParams.DISK_USAGE.key(), query, andBsonList);
        addStringOrQuery("experimentId", QueryParams.EXPERIMENT_ID.key(), query, andBsonList);
        addIntegerOrQuery("jobId", QueryParams.JOB_ID.key(), query, andBsonList);
        addIntegerOrQuery("sampleIds", QueryParams.SAMPLE_IDS.key(), query, andBsonList);
        addStringOrQuery("attributes", QueryParams.ATTRIBUTES.key(), query, andBsonList);
        addIntegerOrQuery("attributes", QueryParams.NATTRIBUTES.key(), query, andBsonList);
        // Fixme: Battributes should be addBooleanOrQuery
        addStringOrQuery("attributes", QueryParams.BATTRIBUTES.key(), query, andBsonList);
        addStringOrQuery("stats", QueryParams.STATS.key(), query, andBsonList);
        addIntegerOrQuery("stats", QueryParams.NSTATS.key(), query, andBsonList);

        addStringOrQuery("acl.userId", QueryParams.ACL_USER_ID.key(), query, andBsonList);
        addStringOrQuery("acl.read", QueryParams.ACL_READ.key(), query, andBsonList);
        addStringOrQuery("acl.write", QueryParams.ACL_WRITE.key(), query, andBsonList);
        addStringOrQuery("acl.execute", QueryParams.ACL_EXECUTE.key(), query, andBsonList);
        addStringOrQuery("acl.delete", QueryParams.ACL_DELETE.key(), query, andBsonList);

        addStringOrQuery("index.userId", QueryParams.INDEX_USER_ID.key(), query, andBsonList);
        addStringOrQuery("index.date", QueryParams.INDEX_DATE.key(), query, andBsonList);
        addStringOrQuery("index.status", QueryParams.INDEX_STATUS.key(), query, andBsonList);
        addIntegerOrQuery("index.jobId", QueryParams.INDEX_JOB_ID.key(), query, andBsonList);

        addIntegerOrQuery(_STUDY_ID, QueryParams.STUDY_ID.key(), query, andBsonList);

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public MongoDBCollection getFileCollection() {
        return fileCollection;
    }
}
