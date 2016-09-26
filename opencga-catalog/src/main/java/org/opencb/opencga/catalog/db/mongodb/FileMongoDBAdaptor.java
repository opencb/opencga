package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.DatasetDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class FileMongoDBAdaptor extends MongoDBAdaptor implements FileDBAdaptor {

    private final MongoDBCollection fileCollection;
    private FileConverter fileConverter;
    private AclMongoDBAdaptor<FileAclEntry> aclDBAdaptor;

    /***
     * CatalogMongoFileDBAdaptor constructor.
     *
     * @param fileCollection MongoDB connection to the file collection.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     */
    public FileMongoDBAdaptor(MongoDBCollection fileCollection, MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(FileMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.fileConverter = new FileConverter();
        this.aclDBAdaptor = new AclMongoDBAdaptor<>(fileCollection, fileConverter, logger);
    }

    /**
     *
     * @return MongoDB connection to the file collection.
     */
    public MongoDBCollection getFileCollection() {
        return fileCollection;
    }

    @Override
    public QueryResult<File> insert(File file, long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        String ownerId = dbAdaptorFactory.getCatalogStudyDBAdaptor().getOwnerId(studyId);

        if (filePathExists(studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath());
        }

        //new File Id
        long newFileId = getNewId();
        file.setId(newFileId);
        Document fileDocument = fileConverter.convertToStorageType(file);
        fileDocument.append(PRIVATE_STUDY_ID, studyId);
        fileDocument.append(PRIVATE_ID, newFileId);

        try {
            fileCollection.insert(fileDocument, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath(), e);
        }

        // Update the diskUsage field from the study collection
        if (!file.isExternal()) {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getDiskUsage());
        }
//        try {
//            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getDiskUsage());
//        } catch (CatalogDBException e) {
//            delete(newFileId, options);
//            throw new CatalogDBException("File from study { id:" + studyId + "} was removed from the database due to problems "
//                    + "with the study collection.");
//        }

        return endQuery("Create file", startTime, get(newFileId, options));
    }

    @Override
    public QueryResult<File> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!=" + File.FileStatus.REMOVED);
        }
        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get file: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_FILES);

        QueryResult<File> fileQueryResult = fileCollection.find(bson, fileConverter, qOptions);
        logger.debug("File get: query : {}, project: {}, dbTime: {}",
                bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), qOptions == null ? "" : qOptions.toJson(),
                fileQueryResult.getDbTime());
        return endQuery("get File", startTime, fileQueryResult);
    }

    @Override
    public QueryResult<File> get(long fileId, QueryOptions options) throws CatalogDBException {
        checkId(fileId);
        Query query = new Query(QueryParams.ID.key(), fileId);
        return get(query, options);
    }

    @Override
    public long getId(long studyId, String path) throws CatalogDBException {
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId).append(QueryParams.PATH.key(), path);
        QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, "id");
        QueryResult<File> fileQueryResult = get(query, options);
        return fileQueryResult.getNumTotalResults() == 1 ? fileQueryResult.getResult().get(0).getId() : -1;
    }

    @Override
    public QueryResult<File> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return get(query, options);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(long studyId, String path, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = Filters.and(Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.regex("path", "^" + path + "[^/]+/?$"));
        List<File> fileResults = fileCollection.find(query, fileConverter, null).getResult();
        return endQuery("Get all files", startTime, fileResults);
    }

    @Override
    public QueryResult<Map<String, Map<String, FileAclEntry>>> getAcls(long studyId, List<String> filePaths, List<String> userIds)
            throws CatalogDBException {

        long startTime = startQuery();
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
//        DBObject match = new BasicDBObject("$match", new BasicDBObject(PRIVATE_STUDY_ID, studyId).append("path", new BasicDBObject("$in",
//                filePaths)));
//        DBObject unwind = new BasicDBObject("$unwind", "$acl");
//        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
//        DBObject project = new BasicDBObject("$project", new BasicDBObject("path", 1).append("id", 1).append("acl", 1));
//        QueryResult<DBObject> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        Bson match = Aggregates.match(Filters.and(Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.in(QueryParams.PATH.key(), filePaths)));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACL.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACL_MEMBER.key(), userIds));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.PATH.key(), QueryParams.ACL.key()));
        QueryResult<Document> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        List<File> files = parseFiles(result);
        Map<String, Map<String, FileAclEntry>> pathAclMap = new HashMap<>();
        for (File file : files) {
//            AclEntry acl = file.getAcl().get(0);
            for (FileAclEntry acl : file.getAcl()) {
                if (pathAclMap.containsKey(file.getPath())) {
                    Map<String, FileAclEntry> userAclMap = pathAclMap.get(file.getPath());
                    if (!userAclMap.containsKey(acl.getMember())) {
                        userAclMap.put(acl.getMember(), acl);
                    }
                } else {
                    HashMap<String, FileAclEntry> userAclMap = new HashMap<>();
                    userAclMap.put(acl.getMember(), acl);
                    pathAclMap.put(file.getPath(), userAclMap);
                }
            }
        }
//        Map<String, Acl> pathAclMap = files.stream().collect(Collectors.toMap(File::getPath, file -> file.getAcl().get(0)));
        logger.debug("getFilesAcl for {} paths and {} users, dbTime: {} ", filePaths.size(), userIds.size(), result.getDbTime());
        return endQuery("getFilesAcl", startTime, Collections.singletonList(pathAclMap));
    }

    @Override
    public long getStudyIdByFileId(long fileId) throws CatalogDBException {
        QueryResult queryResult = nativeGet(new Query(QueryParams.ID.key(), fileId), null);

        if (!queryResult.getResult().isEmpty()) {
            return (long) ((Document) queryResult.getResult().get(0)).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public List<Long> getStudyIdsByFileIds(String fileIds) throws CatalogDBException {
        Bson query = parseQuery(new Query(QueryParams.ID.key(), fileIds));
        return fileCollection.distinct(PRIVATE_STUDY_ID, query, Long.class).getResult();
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
    public QueryResult<File> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        checkId(id);
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
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FILTER_ROUTE_FILES + QueryParams.DISK_USAGE.key(), FILTER_ROUTE_FILES + PRIVATE_STUDY_ID));
            fileQueryResult = nativeGet(query, queryOptions);
        }

        // We perform the update.
        Bson queryBson = parseQuery(query);
        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(),
                QueryParams.MODIFICATION_DATE.key(), QueryParams.PATH.key(), };
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, fileParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = new HashMap<>();
        acceptedEnums.put(QueryParams.TYPE.key(), File.Type.class);
        acceptedEnums.put(QueryParams.FORMAT.key(), File.Format.class);
        acceptedEnums.put(QueryParams.BIOFORMAT.key(), File.Bioformat.class);
        // acceptedEnums.put("fileStatus", File.FileStatusEnum.class);
        try {
            filterEnumParams(parameters, fileParameters, acceptedEnums);
        } catch (CatalogDBException e) {
            e.printStackTrace();
            throw new CatalogDBException("File update: It was impossible updating the files. " + e.getMessage());
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            fileParameters.put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            fileParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.RELATED_FILES.key())) {
            Object o = parameters.get(QueryParams.RELATED_FILES.key());
            if (o instanceof List<?>) {
                List<Document> relatedFiles = new ArrayList<>(((List<?>) o).size());
                for (Object relatedFile : ((List<?>) o)) {
                    relatedFiles.add(getMongoDBDocument(relatedFile, "RelatedFile"));
                }
                fileParameters.put(QueryParams.RELATED_FILES.key(), relatedFiles);
            }
        }
        if (parameters.containsKey(QueryParams.INDEX_TRANSFORMED_FILE.key())) {
            fileParameters.put(QueryParams.INDEX_TRANSFORMED_FILE.key(),
                    getMongoDBDocument(parameters.get(QueryParams.INDEX_TRANSFORMED_FILE.key()), "TransformedFile"));
        }

        String[] acceptedLongParams = {QueryParams.DISK_USAGE.key()};
        filterLongParams(parameters, fileParameters, acceptedLongParams);

        String[] acceptedIntParams = {QueryParams.JOB_ID.key()};
        // Fixme: Add "experiment_id" ?
        filterIntParams(parameters, fileParameters, acceptedIntParams);
        // Check if the job exists.
        if (parameters.containsKey(QueryParams.JOB_ID.key())) {
            if (!this.dbAdaptorFactory.getCatalogJobDBAdaptor().exists(parameters.getInt(QueryParams.JOB_ID.key()))) {
                throw CatalogDBException.idNotFound("Job", parameters.getInt(QueryParams.JOB_ID.key()));
            }
        }

        String[] acceptedIntegerListParams = {QueryParams.SAMPLE_IDS.key()};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);
        // Check if the sample ids exist.
        if (parameters.containsKey(QueryParams.SAMPLE_IDS.key())) {
            for (Integer sampleId : parameters.getAsIntegerList(QueryParams.SAMPLE_IDS.key())) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().exists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, fileParameters, acceptedMapParams);
        // Fixme: Attributes and stats can be also parsed to numeric or boolean

        String[] acceptedObjectParams = {QueryParams.INDEX.key()};
        filterObjectParams(parameters, fileParameters, acceptedObjectParams);

        if (!fileParameters.isEmpty()) {
            QueryResult<UpdateResult> update = fileCollection.update(queryBson, new Document("$set", fileParameters), null);

            // If the diskUsage of some of the files have been changed, notify to the correspondent study
            if (fileQueryResult != null) {
                long newDiskUsage = parameters.getLong(QueryParams.DISK_USAGE.key());
                for (Document file : (List<Document>) fileQueryResult.getResult()) {
                    long difDiskUsage = newDiskUsage - Long.parseLong(file.get(QueryParams.DISK_USAGE.key()).toString());
                    long studyId = (long) file.get(PRIVATE_STUDY_ID);
                    dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, difDiskUsage);
                }
            }
            return endQuery("Update file", startTime, Collections.singletonList(update.getResult().get(0).getModifiedCount()));
        }
        return endQuery("Update file", startTime, Collections.singletonList(0L));
    }

    @Override
    public QueryResult<File> rename(long fileId, String filePath, String fileUri, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        checkId(fileId);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        Document fileDoc = (Document) nativeGet(new Query(QueryParams.ID.key(), fileId), null).getResult().get(0);
        File file = fileConverter.convertToDataModelType(fileDoc);

        long studyId = (long) fileDoc.get(PRIVATE_STUDY_ID);
        long collisionFileId = getId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.DIRECTORY)) {  // recursive over the files inside folder
            QueryResult<File> allFilesInFolder = getAllFilesInFolder(studyId, file.getPath(), null);
            String oldPath = file.getPath();
            filePath += filePath.endsWith("/") ? "" : "/";
            URI uri = file.getUri();
            String oldUri = uri != null ? uri.toString() : "";
            for (File subFile : allFilesInFolder.getResult()) {
                String replacedPath = subFile.getPath().replaceFirst(oldPath, filePath);
                String replacedUri = subFile.getUri().toString().replaceFirst(oldUri, fileUri);
                rename(subFile.getId(), replacedPath, replacedUri, null); // first part of the path in the subfiles 3
            }
        }

        Document query = new Document(PRIVATE_ID, fileId);
        Document set = new Document("$set", new Document()
                .append(QueryParams.NAME.key(), fileName)
                .append(QueryParams.PATH.key(), filePath)
                .append(QueryParams.URI.key(), fileUri));
        QueryResult<UpdateResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return endQuery("Rename file", startTime, get(fileId, options));
    }

    @Override
    public QueryResult<File> delete(long fileId, QueryOptions queryOptions) throws CatalogDBException {
        return delete(fileId, new ObjectMap(QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED), queryOptions);
    }

    @Override
    public QueryResult<File> delete(long fileId, ObjectMap update, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();


        QueryResult<File> file = get(fileId, new QueryOptions(QueryOptions.INCLUDE, FILTER_ROUTE_FILES + QueryParams.STATUS.key()));
        if (file == null || file.getNumResults() == 0) {
            throw CatalogDBException.idNotFound("file", fileId);
        }

        String status = file.first().getStatus().getName();

        boolean skipCheckCanDelete = update.getBoolean(SKIP_CHECK, false);
        if (status.equalsIgnoreCase(File.FileStatus.READY)) {
            if (!skipCheckCanDelete) {
                checkCanDelete(fileId);
            } else {
                deleteReferencesToFile(fileId);
            }
        }

        update(fileId, update);

        Query query = new Query(QueryParams.ID.key(), fileId);
        if (update.containsKey(QueryParams.STATUS_NAME.key())) {
            query.append(QueryParams.STATUS_NAME.key(), update.getString(QueryParams.STATUS_NAME.key()));
        }

        return endQuery("Delete file", startTime, get(query, queryOptions));
    }

//    @Override
//    public QueryResult<File> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
//        long startTime = startQuery();
//
//        checkFileId(id);
//        // Check the file is active
//        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!="
//                + File.FileStatus.DELETED);
//        if (count(query).first() == 0) {
//            query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED + "," + Status.DELETED);
//            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_NAME.key());
//            File file = get(query, options).first();
//            throw new CatalogDBException("The file {" + id + "} was already " + file.getName().getName());
//        }
//
//        // If we don't find the force parameter, we check first if the file could be deleted.
//        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
//            checkCanDelete(id);
//        }
//
//        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
//            deleteReferencesToFile(id);
//        }
//
//        // Change the status of the project to deleted
//        setName(id, Status.TRASHED);
//
//        query = new Query(QueryParams.ID.key(), id)
//                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
//
//        return endQuery("Delete file", startTime, get(query, queryOptions));
//    }

    // TODO: Think
    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_NAME.key(), Status.READY);
        QueryResult<File> fileQueryResult = get(query, new QueryOptions(QueryOptions.INCLUDE, QueryParams.ID.key()));
        for (File file : fileQueryResult.getResult()) {
            delete(file.getId(), queryOptions);
        }
        return endQuery("Delete file", startTime, Collections.singletonList(fileQueryResult.getNumTotalResults()));
    }

    // TODO: Think
    @Override
    @Deprecated
    public QueryResult<File> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
//        long startTime = startQuery();
//        checkFileId(id);
//        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.REMOVED);
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
//                FILTER_ROUTE_FILES + QueryParams.ID.key() + "," + FILTER_ROUTE_FILES + QueryParams.STATUS.key());
//        QueryResult<File> fileQueryResult = get(query, options);
//        return endQuery("Remove file", startTime,
//                remove(fileQueryResult.first().getId(), fileQueryResult.first().getName().getName(), queryOptions));
    }

    // TODO: Think
    @Override
    @Deprecated
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
//        long startTime = startQuery();
//        // In case there is a status set in the query, we take it out.
//        query.put(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.REMOVED);
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
//                FILTER_ROUTE_FILES + QueryParams.ID.key() + "," + FILTER_ROUTE_FILES + QueryParams.STATUS.key());
//        QueryResult<File> fileQueryResult = get(query, options);
//
//        for (File file : fileQueryResult.getResult()) {
//            remove(file.getId(), file.getName().getName(), queryOptions);
//        }
//        return endQuery("Remove file", startTime, Collections.singletonList(fileQueryResult.getNumTotalResults()));
    }

    @Deprecated
    private QueryResult<File> remove(long fileId, String currentStatus, QueryOptions queryOptions) throws CatalogDBException {
        return null;
//        long startTime = startQuery();
//        if (currentStatus.equals(File.FileStatus.DELETED)) { // We can change the status directly to Removed !
//            setName(fileId, File.FileStatus.REMOVED);
//       } else if (!currentStatus.equals(File.FileStatus.REMOVED)) { // If it's different from removed, we have to take care of everything.
//
//            if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
//                checkCanDelete(fileId);
//            }
//
//            if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
//                deleteReferencesToFile(fileId);
//            }
//
//            // Change the status of the file to removed
//            setName(fileId, File.FileStatus.REMOVED);
//        }
//
//        Query query = new Query(QueryParams.ID.key(), fileId).append(QueryParams.STATUS_NAME.key(), Status.DELETED);
//        return endQuery("Remove file", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        return endQuery("Restore files", startTime, setStatus(query, File.FileStatus.READY));
    }

    @Override
    public QueryResult<File> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), Status.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The file {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, File.FileStatus.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore file", startTime, get(query, null));
    }

    public QueryResult<File> clean(int id) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<File> file = get(id, new QueryOptions());
        QueryResult<DeleteResult> deleteResult = fileCollection.remove(new Document(QueryParams.ID.key(), id), null);
        if (deleteResult.getNumResults() == 1) {
            return endQuery("Delete file", startTime, file);
        } else {
            throw CatalogDBException.deleteError("File");
        }
    }

    public void checkFileNotInUse(long fileId) throws CatalogDBException {
        Query query = new Query(JobDBAdaptor.QueryParams.INPUT.key(), fileId);
        QueryResult<Long> count = dbAdaptorFactory.getCatalogJobDBAdaptor().count(query);

        if (count.first() > 0) {
            throw CatalogDBException.fileInUse(fileId, count.getNumResults());
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
    public DBIterator<File> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, fileConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
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
        DBIterator<File> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    // Auxiliar methods

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case DIRECTORY:
                        // We add the regex in order to look for all the files under the given directory
                        String value = (String) query.get(queryParam.key());
                        String regExPath = "~^" + value + "[^/]+/?$";
                        Query pathQuery = new Query(QueryParams.PATH.key(), regExPath);
                        addAutoOrQuery(QueryParams.PATH.key(), QueryParams.PATH.key(), pathQuery, QueryParams.PATH.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    private boolean filePathExists(long studyId, String path) {
        Document query = new Document(PRIVATE_STUDY_ID, studyId).append(QueryParams.PATH.key(), path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    // TODO: Check these deprecated methods and get rid of them at some point

    @Deprecated
    public QueryResult<File> getAllFiles(Query query, QueryOptions options) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use get instead.");
/*
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
                        addCompQueryFilter(option, option.name(), query, PRIVATE_ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option, option.name(), query, PRIVATE_STUDY_ID, mongoQueryList);
                        break;
                    case directory:
                        mongoQueryList.add(new BasicDBObject("path", new BasicDBObject("$regex", "^" + query.getString("directory") +
                                "[^/]+/?$")));
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

        BasicDBObject mongoQuery = new BasicDBObject("$and", mongoQueryList);
        QueryOptions queryOptions = filterOptions(options, FILTER_ROUTE_FILES);
//        QueryResult<DBObject> queryResult = fileCollection.find(mongoQuery, null, File.class, queryOptions);
        QueryResult<File> queryResult = fileCollection.find(mongoQuery, null, new ComplexTypeConverter<File, DBObject>() {
            @Override
            public File convertToDataModelType(DBObject object) {
                try {
                    return getObjectReader(File.class).readValue(restoreDotsInKeys(object).toString());
                } catch (IOException e) {
                    return null;
                }
            }

            @Override
            public DBObject convertToStorageType(File object) {
                return null;
            }
        }, queryOptions);
        logger.debug("File search: query : {}, project: {}, dbTime: {}", mongoQuery, queryOptions == null ? "" : queryOptions.toJson(),
                queryResult.getDbTime());
//        List<File> files = parseFiles(queryResult);

        return endQuery("Search File", startTime, queryResult);
        */
    }

    QueryResult<File> setStatus(long fileId, String status) throws CatalogDBException {
        return update(fileId, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status));
    }

    /**
     * Checks whether the fileId is being referred on any other document.
     *
     * @param fileId file id.
     * @throws CatalogDBException when the fileId is being used as input of any job or dataset.
     */
    private void checkCanDelete(long fileId) throws CatalogDBException {

        // Check if the file is being used as input of any job
        Query query = new Query(JobDBAdaptor.QueryParams.INPUT.key(), fileId);
        Long count = dbAdaptorFactory.getCatalogJobDBAdaptor().count(query).first();
        if ((count > 0)) {
            throw new CatalogDBException("The file " + fileId + " cannot be deleted/removed because it is being used as input of "
                    + count + " jobs. Please, consider using the parameter force if you are sure you want to delete it.");
        }

        query = new Query(DatasetDBAdaptor.QueryParams.FILES.key(), fileId);
        count = dbAdaptorFactory.getCatalogDatasetDBAdaptor().count(query).first();
        if ((count > 0)) {
            throw new CatalogDBException("The file " + fileId + " cannot be deleted/removed because it is part of "
                    + count + " dataset(s). Please, consider using the parameter force if you are sure you want to delete it.");
        }

    }

    /**
     * Remove the references from active documents that are pointing to the current fileId.
     *
     * @param fileId file Id.
     * @throws CatalogDBException when there is any kind of error.
     */
    private void deleteReferencesToFile(long fileId) throws CatalogDBException {
        // Remove references from datasets
        Query query = new Query(DatasetDBAdaptor.QueryParams.FILES.key(), fileId);
        QueryResult<Long> result = dbAdaptorFactory.getCatalogDatasetDBAdaptor()
                .extractFilesFromDatasets(query, Collections.singletonList(fileId));
        logger.debug("FileId {} extracted from {} datasets", fileId, result.first());

        // Remove references from jobs
        result = dbAdaptorFactory.getCatalogJobDBAdaptor().extractFiles(Collections.singletonList(fileId));
        logger.debug("FileId {} extracted from {} jobs", fileId, result.first());
    }

    public QueryResult<Long> extractSampleFromFiles(Query query, List<Long> sampleIds) throws CatalogDBException {
        long startTime = startQuery();
        Bson bsonQuery = parseQuery(query);
        Bson update = new Document("$pull", new Document(QueryParams.SAMPLE_IDS.key(), new Document("$in", sampleIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> updateQueryResult = fileCollection.update(bsonQuery, update, multi);
        return endQuery("Extract samples from files", startTime, Collections.singletonList(updateQueryResult.first().getModifiedCount()));
    }

    @Override
    public QueryResult<FileAclEntry> createAcl(long id, FileAclEntry acl) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.createAcl(id, acl, fileCollection, "FileAcl");
        return endQuery("create file Acl", startTime, Arrays.asList(aclDBAdaptor.createAcl(id, acl)));
    }

    @Override
    public QueryResult<FileAclEntry> getAcl(long id, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
//
//        List<FileAclEntry> acl = null;
//        QueryResult<Document> aggregate = CatalogMongoDBUtils.getAcl(id, members, fileCollection, logger);
//        File file = fileConverter.convertToDataModelType(aggregate.first());
//
//        if (file != null) {
//            acl = file.getAcl();
//        }

        return endQuery("get file Acl", startTime, aclDBAdaptor.getAcl(id, members));
    }

    @Override
    public void removeAcl(long id, String member) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAcl(id, member, fileCollection);
        aclDBAdaptor.removeAcl(id, member);
    }

    @Override
    public QueryResult<FileAclEntry> setAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.setAclsToMember(id, member, permissions, fileCollection);
        return endQuery("Set Acls to member", startTime, Arrays.asList(aclDBAdaptor.setAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<FileAclEntry> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException {
        long startTime = startQuery();
//        CatalogMongoDBUtils.addAclsToMember(id, member, permissions, fileCollection);
        return endQuery("Add Acls to member", startTime, Arrays.asList(aclDBAdaptor.addAclsToMember(id, member, permissions)));
    }

    @Override
    public QueryResult<FileAclEntry> removeAclsFromMember(long id, String member, List<String> permissions) throws CatalogDBException {
//        CatalogMongoDBUtils.removeAclsFromMember(id, member, permissions, fileCollection);
        long startTime = startQuery();
        return endQuery("Remove Acls from member", startTime, Arrays.asList(aclDBAdaptor.removeAclsFromMember(id, member, permissions)));
    }

    public void removeAclsFromStudy(long studyId, String member) throws CatalogDBException {
        aclDBAdaptor.removeAclsFromStudy(studyId, member);
    }
}
