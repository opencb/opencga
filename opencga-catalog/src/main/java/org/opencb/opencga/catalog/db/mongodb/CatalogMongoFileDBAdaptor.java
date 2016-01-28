package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCursor;
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
import org.opencb.opencga.catalog.db.api.CatalogDBIterator;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
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

    private final MongoDBCollection fileCollection;
    private FileConverter fileConverter;

    public CatalogMongoFileDBAdaptor(MongoDBCollection fileCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoFileDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.fileConverter = new FileConverter();
    }

    private boolean filePathExists(int studyId, String path) {
        Document query = new Document(PRIVATE_STUDY_ID, studyId).append(QueryParams.PATH.key(), path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
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
        Document fileDocument = fileConverter.convertToStorageType(file);
        fileDocument.append(PRIVATE_STUDY_ID, studyId);
        fileDocument.append(PRIVATE_ID, newFileId);

        try {
            fileCollection.insert(fileDocument, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        // Update the diskUsage field from the study collection
        try {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getDiskUsage());
        } catch (CatalogDBException e) {
            deleteFile(newFileId);
            throw new CatalogDBException("File from study { id:" + studyId + "} was removed from the database due to problems "
                    + "with the study collection.");
        }

        return endQuery("Create file", startTime, getFile(newFileId, options));
    }

    /**
     * At the moment it does not clean external references to itself.
     */
    @Deprecated
    @Override
    public QueryResult<File> deleteFile(int fileId) throws CatalogDBException {
        return delete(fileId);
    }

    @Override
    public int getFileId(int studyId, String path) throws CatalogDBException {
        Query query = new Query(PRIVATE_STUDY_ID, studyId).append(QueryParams.PATH.key(), path);
        QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, "id");
        QueryResult<File> fileQueryResult = get(query, options);
        return fileQueryResult.getNumTotalResults() == 1 ? fileQueryResult.getResult().get(0).getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFilesInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        Query query = new Query(PRIVATE_STUDY_ID, studyId);
        return get(query, options);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(int studyId, String path, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = Filters.and(Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.regex("path", "^" + path + "[^/]+/?$"));
        List<File> fileResults = fileCollection.find(query, fileConverter, null).getResult();
        return endQuery("Get all files", startTime, fileResults);
    }

    @Override
    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException {
        checkFileId(fileId);
        Query query = new Query(PRIVATE_ID, fileId);
        return get(query, options);
    }

    @Deprecated
    @Override
    public QueryResult<File> modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException {
        return update(fileId, parameters);
    }


    /**
     * @param filePath assuming 'pathRelativeToStudy + name'
     * @param options
     */
    @Override
    public QueryResult<File> renameFile(int fileId, String filePath, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        checkFileId(fileId);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        Document fileDoc = (Document) nativeGet(new Query(PRIVATE_ID, fileId), null).getResult().get(0);
        File file = fileConverter.convertToDataModelType(fileDoc);

        int studyId = (int) fileDoc.get(PRIVATE_STUDY_ID);
        int collisionFileId = getFileId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.FOLDER)) {  // recursive over the files inside folder
            QueryResult<File> allFilesInFolder = getAllFilesInFolder(studyId, file.getPath(), null);
            String oldPath = file.getPath();
            filePath += filePath.endsWith("/") ? "" : "/";
            for (File subFile : allFilesInFolder.getResult()) {
                String replacedPath = subFile.getPath().replaceFirst(oldPath, filePath);
                renameFile(subFile.getId(), replacedPath, null); // first part of the path in the subfiles 3
            }
        }

        Document query = new Document(PRIVATE_ID, fileId);
        Document set = new Document("$set", new Document("name", fileName).append("path", filePath));
        QueryResult<UpdateResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return endQuery("Rename file", startTime, getFile(fileId, options));
    }

    @Override
    public int getStudyIdByFileId(int fileId) throws CatalogDBException {
        QueryResult queryResult = nativeGet(new Query(PRIVATE_ID, fileId), null);

        if (!queryResult.getResult().isEmpty()) {
            return (int) ((Document) queryResult.getResult().get(0)).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public String getFileOwnerId(int fileId) throws CatalogDBException {
        return getFile(fileId, null).getResult().get(0).getOwnerId();
    }

    /*
     * query: db.file.find({id:2}, {acl:{$elemMatch:{userId:"jcoll"}}, studyId:1})
     */
    @Override
    public QueryResult<AclEntry> getFileAcl(int fileId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(PRIVATE_ID, fileId);
        Bson projection = Projections.elemMatch("acl", Filters.eq("userId", userId));
        QueryOptions options = new QueryOptions(MongoDBCollection.ELEM_MATCH, projection);
/*
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put("elemMatch", Projections.elemMatch("acl", Filters.eq("userId", userId)));
        queryOptions.put("include", Projections.include("acl", "userId"));
*/

        List<AclEntry> acl = get(query, options).getResult().get(0).getAcl();
        return endQuery("Get file ACL", startTime, acl);
    }

    @Override
    public QueryResult<Map<String, Map<String, AclEntry>>> getFilesAcl(int studyId, List<String> filePaths, List<String> userIds) throws
            CatalogDBException {

        long startTime = startQuery();
//        DBObject match = new BasicDBObject("$match", new BasicDBObject(PRIVATE_STUDY_ID, studyId).append("path", new BasicDBObject("$in",
//                filePaths)));
//        DBObject unwind = new BasicDBObject("$unwind", "$acl");
//        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
//        DBObject project = new BasicDBObject("$project", new BasicDBObject("path", 1).append("id", 1).append("acl", 1));
//        QueryResult<DBObject> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        Bson match = Aggregates.match(Filters.and(Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.in("acl.userId", userIds)));
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
            //query = new BasicDBObject(PRIVATE_ID, fileId);
            // update = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
            query = Filters.eq(PRIVATE_ID, fileId);
            update = Updates.push("acl", newAclObject);
        } else {    // there is already another ACL: overwrite
            /*query = BasicDBObjectBuilder
                    .start(PRIVATE_ID, fileId)
                    .append("acl.userId", userId).get();
            update = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));*/
            query = Filters.and(Filters.eq(PRIVATE_ID, fileId), Filters.eq("acl.userId", userId));
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
//        DBObject query = new BasicDBObject(PRIVATE_ID, fileId);
//        DBObject update = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", userId)));
//        QueryResult queryResult = fileCollection.update(query, update, null);

        Bson query = Filters.eq(PRIVATE_ID, fileId);
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
//                        addCompQueryFilter(option, option.name(), query, PRIVATE_ID, mongoQueryList);
//                        break;
//                    case studyId:
//                        addCompQueryFilter(option, option.name(), query, PRIVATE_STUDY_ID, mongoQueryList);
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
//                .start(PRIVATE_ID, studyId)
//                .append("datasets.name", dataset.getName())
//                .get());

        Query query = new Query(PRIVATE_ID, studyId)
                .append("datasets.name", dataset.getName());
        QueryResult<Long> count = dbAdaptorFactory.getCatalogStudyDBAdaptor().count(query);

        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Dataset { name: \"" + dataset.getName() + "\" } already exists in this study.");
        }

        int newId = getNewId();
        dataset.setId(newId);

        Document datasetObject = getMongoDBDocument(dataset, "Dataset");
        QueryResult<UpdateResult> update = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()
                .update(Filters.eq(PRIVATE_ID, studyId), new BasicDBObject("$push", new BasicDBObject("datasets", datasetObject)), null);

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
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return fileCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return fileCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<File> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_FILES);

        QueryResult<File> fileQueryResult = fileCollection.find(bson, fileConverter, qOptions);
        return endQuery("get File", startTime, fileQueryResult.getResult());
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_FILES);

        return fileCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<File> update(int id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        checkFileId(id);
        Query query = new Query(QueryParams.ID.key(), id);
        update(query, parameters);
        return endQuery("Update file", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        // If the user wants to change the diskUsages of the file(s), we first make a query to obtain the old values.
        QueryResult fileQueryResult = null;
        if (parameters.containsKey(QueryParams.DISK_USAGE.key())) {
            QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(
                    FILTER_ROUTE_FILES + QueryParams.DISK_USAGE.key(), FILTER_ROUTE_FILES + PRIVATE_STUDY_ID));
            fileQueryResult = nativeGet(query, queryOptions);
        }

        // We perform the update.
        Bson queryBson = parseQuery(query);
        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(),
                QueryParams.MODIFICATION_DATE.key(), };
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

            // If the diskUsage of some of the files have been changed, notify to the correspondent study
            if (fileQueryResult != null) {
                long newDiskUsage = parameters.getLong(QueryParams.DISK_USAGE.key());
                for (Document file : (List<Document>) fileQueryResult.getResult()) {
                    long difDiskUsage = newDiskUsage - Long.parseLong(file.get(QueryParams.DISK_USAGE.key()).toString());
                    int studyId = (int) file.get(PRIVATE_STUDY_ID);
                    dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, difDiskUsage);
                }
            }
            return endQuery("Update file", startTime, Collections.singletonList(update.getResult().get(0).getModifiedCount()));
        }
        return endQuery("Update file", startTime, Collections.singletonList(0L));
    }

    @Override
    public QueryResult<File> delete(int id) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<File> file = getFile(id, null);
        Bson query = new Document(PRIVATE_ID, id);
        DeleteResult removedFile = fileCollection.remove(query, null).first();
        if (removedFile.getDeletedCount() != 1) {
            throw new CatalogDBException("Could not remove file " + id);
        }
        return endQuery("Delete file", startTime, file.getResult());
    }

    @Override
    public QueryResult<Long> delete(Query query) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<DeleteResult> deleteResult = fileCollection.remove(parseQuery(query), null);
        return endQuery("Delete file", startTime, Collections.singletonList(deleteResult.first().getDeletedCount()));
    }

    @Override
    public CatalogDBIterator<File> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, fileConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(fileCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<File> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        // FIXME: Pedro. Check the mongodb names as well as integer createQueries
        addIntegerOrQuery(PRIVATE_ID, PRIVATE_ID, query, andBsonList);
        addIntegerOrQuery(QueryParams.ID.key(), QueryParams.ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.NAME.key(), QueryParams.NAME.key(), query, andBsonList);
        addStringOrQuery(QueryParams.TYPE.key(), QueryParams.TYPE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.FORMAT.key(), QueryParams.FORMAT.key(), query, andBsonList);
        addStringOrQuery(QueryParams.BIOFORMAT.key(), QueryParams.BIOFORMAT.key(), query, andBsonList);
        addStringOrQuery(QueryParams.URI.key(), QueryParams.URI.key(), query, andBsonList);
        addStringOrQuery(QueryParams.DELETE_DATE.key(), QueryParams.DELETE_DATE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.OWNER_ID.key(), QueryParams.OWNER_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.CREATION_DATE.key(), QueryParams.CREATION_DATE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.MODIFICATION_DATE.key(), QueryParams.MODIFICATION_DATE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.DESCRIPTION.key(), QueryParams.DESCRIPTION.key(), query, andBsonList);
        addStringOrQuery(QueryParams.STATUS.key(), QueryParams.STATUS.key(), query, andBsonList);
        addStringOrQuery(QueryParams.DISK_USAGE.key(), QueryParams.DISK_USAGE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.EXPERIMENT_ID.key(), QueryParams.EXPERIMENT_ID.key(), query, andBsonList);
        addIntegerOrQuery(QueryParams.JOB_ID.key(), QueryParams.JOB_ID.key(), query, andBsonList);
        addIntegerOrQuery(QueryParams.SAMPLE_IDS.key(), QueryParams.SAMPLE_IDS.key(), query, andBsonList);
        addStringOrQuery(QueryParams.ATTRIBUTES.key(), QueryParams.ATTRIBUTES.key(), query, andBsonList);
        addIntegerOrQuery(QueryParams.ATTRIBUTES.key(), QueryParams.NATTRIBUTES.key(), query, andBsonList);
        // Fixme: Battributes should be addBooleanOrQuery
        addStringOrQuery(QueryParams.ATTRIBUTES.key(), QueryParams.BATTRIBUTES.key(), query, andBsonList);
        addStringOrQuery(QueryParams.STATS.key(), QueryParams.STATS.key(), query, andBsonList);
        addIntegerOrQuery(QueryParams.NSTATS.key(), QueryParams.NSTATS.key(), query, andBsonList);
        addStringOrQuery(QueryParams.PATH.key(), QueryParams.PATH.key(), query, andBsonList);

        addStringOrQuery(QueryParams.ACL_USER_ID.key(), QueryParams.ACL_USER_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.ACL_READ.key(), QueryParams.ACL_READ.key(), query, andBsonList);
        addStringOrQuery(QueryParams.ACL_WRITE.key(), QueryParams.ACL_WRITE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.ACL_EXECUTE.key(), QueryParams.ACL_EXECUTE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.ACL_DELETE.key(), QueryParams.ACL_DELETE.key(), query, andBsonList);

        addStringOrQuery(QueryParams.INDEX_USER_ID.key(), QueryParams.INDEX_USER_ID.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDEX_DATE.key(), QueryParams.INDEX_DATE.key(), query, andBsonList);
        addStringOrQuery(QueryParams.INDEX_STATUS.key(), QueryParams.INDEX_STATUS.key(), query, andBsonList);
        addIntegerOrQuery(QueryParams.INDEX_JOB_ID.key(), QueryParams.INDEX_JOB_ID.key(), query, andBsonList);

        addIntegerOrQuery(PRIVATE_STUDY_ID, PRIVATE_STUDY_ID, query, andBsonList);
        addIntegerOrQuery(PRIVATE_STUDY_ID, QueryParams.STUDY_ID.key(), query, andBsonList);

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
