/*
 * Copyright 2015-2016 OpenCB
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
//        String ownerId = dbAdaptorFactory.getCatalogStudyDBAdaptor().getOwnerId(studyId);

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

        // Update the size field from the study collection
        if (!file.isExternal()) {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getSize());
        }
//        try {
//            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getSize());
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
            bson = parseQuery(query, false);
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

        QueryResult<File> fileQueryResult;
        // TODO: Add the lookup for experiments
        if (qOptions.get("lazy") != null && !qOptions.getBoolean("lazy")) {
            Bson match = Aggregates.match(bson);
            Bson lookup = Aggregates.lookup("job", QueryParams.JOB_ID.key(), JobDBAdaptor.QueryParams.ID.key(), "job");
            fileQueryResult = fileCollection.aggregate(Arrays.asList(match, lookup), fileConverter, qOptions);
        } else {
            fileQueryResult = fileCollection.find(bson, fileConverter, qOptions);
        }
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
        Bson query = parseQuery(new Query(QueryParams.ID.key(), fileIds), false);
        return fileCollection.distinct(PRIVATE_STUDY_ID, query, Long.class).getResult();
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
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
        Bson query = parseQuery(new Query(QueryParams.ID.key(), id), true);
        Map<String, Object> myParams = getValidatedUpdateParams(parameters);

        if (myParams.isEmpty()) {
            logger.debug("The map of parameters to update file is empty. Originally it contained {}", parameters.safeToString());
            throw new CatalogDBException("Nothing to update");
        }

        logger.debug("Update file. Query: {}, Update: {}",
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), myParams);

        QueryResult<UpdateResult> update = fileCollection.update(query, new Document("$set", myParams), new QueryOptions("multi", true));
        if (update.first().getMatchedCount() == 0) {
            throw new CatalogDBException("File " + id + " not found.");
        }

        QueryResult<File> queryResult = fileCollection.find(query, fileConverter, QueryOptions.empty());
        return endQuery("Update file", startTime, queryResult);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        // If the user wants to change the diskUsages of the file(s), we first make a query to obtain the old values.
        QueryResult fileQueryResult = null;
        if (parameters.containsKey(QueryParams.SIZE.key())) {
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FILTER_ROUTE_FILES + QueryParams.SIZE.key(), FILTER_ROUTE_FILES + PRIVATE_STUDY_ID));
            fileQueryResult = nativeGet(query, queryOptions);
        }

        // We perform the update.
        Bson queryBson = parseQuery(query, true);
        Map<String, Object> fileParameters = getValidatedUpdateParams(parameters);

        if (!fileParameters.isEmpty()) {
            logger.debug("Update file. Query: {}, Update: {}",
                    queryBson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), fileParameters);

            QueryResult<UpdateResult> update = fileCollection.update(queryBson, new Document("$set", fileParameters),
                    new QueryOptions("multi", true));

            // If the size of some of the files have been changed, notify to the correspondent study
            if (fileQueryResult != null) {
                long newDiskUsage = parameters.getLong(QueryParams.SIZE.key());
                for (Document file : (List<Document>) fileQueryResult.getResult()) {
                    long difDiskUsage = newDiskUsage - Long.parseLong(file.get(QueryParams.SIZE.key()).toString());
                    long studyId = (long) file.get(PRIVATE_STUDY_ID);
                    dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, difDiskUsage);
                }
            }
            return endQuery("Update file", startTime, Collections.singletonList(update.getResult().get(0).getModifiedCount()));
        }
        return endQuery("Update file", startTime, Collections.singletonList(0L));
    }

    private Map<String, Object> getValidatedUpdateParams(ObjectMap parameters) throws CatalogDBException {
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
            logger.error("Error updating files", e);
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

        String[] acceptedLongParams = {QueryParams.SIZE.key()};
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
        return fileParameters;
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.ID.key(), id);
        delete(query);
    }

    @Override
    public void delete(Query query) throws CatalogDBException {
        QueryResult<DeleteResult> remove = fileCollection.remove(parseQuery(query, false), null);

        if (remove.first().getDeletedCount() == 0) {
            throw CatalogDBException.deleteError("File");
        }
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

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return fileCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query, false);
        return fileCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public DBIterator<File> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator, fileConverter);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new MongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return rank(fileCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(fileCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query, false);
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

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                continue;
            }
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
                    // Other parameter that can be queried.
                    case NAME:
                    case TYPE:
                    case FORMAT:
                    case BIOFORMAT:
                    case URI:
                    case PATH:
                    case CREATION_DATE:
                    case MODIFICATION_DATE:
                    case DESCRIPTION:
                    case EXTERNAL:
                    case STATUS:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case RELATED_FILES:
                    case RELATED_FILES_RELATION:
                    case SIZE:
                    case EXPERIMENT_ID:
                    case SAMPLE_IDS:
                    case JOB_ID:
                    case ACL:
                    case ACL_MEMBER:
                    case ACL_PERMISSIONS:
                    case INDEX:
                    case INDEX_USER_ID:
                    case INDEX_CREATION_DATE:
                    case INDEX_STATUS_NAME:
                    case INDEX_STATUS_MESSAGE:
                    case INDEX_JOB_ID:
                    case INDEX_TRANSFORMED_FILE:
                    case STATS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    default:
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
                    + count + " jobs.");
        }

        query = new Query(DatasetDBAdaptor.QueryParams.FILES.key(), fileId);
        count = dbAdaptorFactory.getCatalogDatasetDBAdaptor().count(query).first();
        if ((count > 0)) {
            throw new CatalogDBException("The file " + fileId + " cannot be deleted/removed because it is part of "
                    + count + " dataset(s).");
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
        Bson bsonQuery = parseQuery(query, true);
        Bson update = new Document("$pull", new Document(QueryParams.SAMPLE_IDS.key(), new Document("$in", sampleIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> updateQueryResult = fileCollection.update(bsonQuery, update, multi);
        return endQuery("Extract samples from files", startTime, Collections.singletonList(updateQueryResult.first().getModifiedCount()));
    }

}
