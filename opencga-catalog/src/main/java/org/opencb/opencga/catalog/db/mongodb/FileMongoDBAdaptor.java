/*
 * Copyright 2015-2017 OpenCB
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

import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.FileMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class FileMongoDBAdaptor extends AnnotationMongoDBAdaptor<File> implements FileDBAdaptor {

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

    @Override
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return fileConverter;
    }

    @Override
    protected MongoDBCollection getCollection() {
        return fileCollection;
    }

    @Override
    public void nativeInsert(Map<String, Object> file, String userId) throws CatalogDBException {
        Document fileDocument = getMongoDBDocument(file, "sample");
        fileCollection.insert(fileDocument, null);
    }

    @Override
    public QueryResult<File> insert(long studyId, File file, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);

        if (filePathExists(studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath());
        }

        //new File Id
        long newFileId = getNewId();
        file.setUid(newFileId);
        file.setStudyUid(studyId);
        if (StringUtils.isEmpty(file.getUuid())) {
            file.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
        }

        Document fileDocument = fileConverter.convertToStorageType(file, variableSetList);
        if (StringUtils.isNotEmpty(file.getCreationDate())) {
            fileDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(file.getCreationDate()));
        } else {
            fileDocument.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        fileDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        try {
            fileCollection.insert(fileDocument, null);
        } catch (MongoWriteException e) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath(), e);
        }

        // Update the size field from the study collection
        if (!file.isExternal()) {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getSize());
        }

        return endQuery("Create file", startTime, get(newFileId, options));
    }

    @Override
    public long getId(long studyId, String path) throws CatalogDBException {
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId).append(QueryParams.PATH.key(), path);
        QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_UID);
        QueryResult<File> fileQueryResult = get(query, options);
        return fileQueryResult.getNumTotalResults() == 1 ? fileQueryResult.getResult().get(0).getUid() : -1;
    }

    @Override
    public QueryResult<File> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId);
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
    public long getStudyIdByFileId(long fileId) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.UID.key(), fileId)
                .append(QueryParams.STATUS_NAME.key(), "!=null");
        QueryResult queryResult = nativeGet(query, null);

        if (!queryResult.getResult().isEmpty()) {
            return (long) ((Document) queryResult.getResult().get(0)).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.uidNotFound("File", fileId);
        }
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        QueryResult<File> fileQueryResult = get(id, queryOptions);
        if (fileQueryResult.first().getAnnotationSets().isEmpty()) {
            return new QueryResult<>("Get annotation set", fileQueryResult.getDbTime(), 0, 0, fileQueryResult.getWarningMsg(),
                    fileQueryResult.getErrorMsg(), Collections.emptyList());
        } else {
            List<AnnotationSet> annotationSets = fileQueryResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new QueryResult<>("Get annotation set", fileQueryResult.getDbTime(), size, size, fileQueryResult.getWarningMsg(),
                    fileQueryResult.getErrorMsg(), annotationSets);
        }
    }

    @Override
    public QueryResult<File> update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();
        Bson query = parseQuery(new Query(QueryParams.UID.key(), id), false);
        UpdateDocument tmpUpdateDocument = getValidatedUpdateParams(parameters, queryOptions);

        updateAnnotationSets(id, parameters, variableSetList, queryOptions, false);

        Document updateDocument = tmpUpdateDocument.toFinalUpdateDocument();
        if (!updateDocument.isEmpty()) {
            logger.debug("Update file. Query: {}, Update: {}", query.toBsonDocument(Document.class,
                    MongoClient.getDefaultCodecRegistry()), updateDocument);

            QueryResult<UpdateResult> update = fileCollection.update(query, updateDocument, new QueryOptions("multi", true));
            if (update.first().getMatchedCount() == 0) {
                throw new CatalogDBException("File " + id + " not found.");
            }
        }

        QueryResult<File> queryResult = fileCollection.find(query, fileConverter, QueryOptions.empty());
        return endQuery("Update file", startTime, queryResult);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();

        // If the user wants to change the diskUsages of the file(s), we first make a query to obtain the old values.
        QueryResult fileQueryResult = null;
        if (parameters.containsKey(QueryParams.SIZE.key())) {
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FILTER_ROUTE_FILES + QueryParams.SIZE.key(), FILTER_ROUTE_FILES + PRIVATE_STUDY_ID));
            fileQueryResult = nativeGet(query, options);
        }

        // We perform the update.
        Bson queryBson = parseQuery(query, false);
        UpdateDocument tmpUpdateDocument = getValidatedUpdateParams(parameters, queryOptions);

        updateAnnotationSets(query.getLong(QueryParams.UID.key(), -1L), parameters, variableSetList, queryOptions, false);

        Document updateDocument = tmpUpdateDocument.toFinalUpdateDocument();

        if (!updateDocument.isEmpty()) {
            logger.debug("Update file. Query: {}, Update: {}",
                    queryBson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), updateDocument);

            QueryResult<UpdateResult> update = fileCollection.update(queryBson, updateDocument, new QueryOptions("multi", true));

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

    @Override
    public QueryResult<File> update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    private UpdateDocument getValidatedUpdateParams(ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(), QueryParams.PATH.key(),
                QueryParams.CHECKSUM.key(),
        };
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (document.getSet().containsKey(QueryParams.PATH.key())) {
            // We also update the ID replacing the / for :
            String path = parameters.getString(QueryParams.PATH.key());
            document.getSet().put(QueryParams.ID.key(), StringUtils.replace(path, "/", ":"));
        }

        String[] acceptedParamsList = { QueryParams.TAGS.key() };
        filterStringListParams(parameters, document.getSet(), acceptedParamsList);

        Map<String, Class<? extends Enum>> acceptedEnums = new HashMap<>();
        acceptedEnums.put(QueryParams.TYPE.key(), File.Type.class);
        acceptedEnums.put(QueryParams.FORMAT.key(), File.Format.class);
        acceptedEnums.put(QueryParams.BIOFORMAT.key(), File.Bioformat.class);
        // acceptedEnums.put("fileStatus", File.FileStatusEnum.class);
        try {
            filterEnumParams(parameters, document.getSet(), acceptedEnums);
        } catch (CatalogDBException e) {
            logger.error("Error updating files", e);
            throw new CatalogDBException("File update: It was impossible updating the files. " + e.getMessage());
        }

        if (parameters.containsKey(QueryParams.STATUS_NAME.key())) {
            document.getSet().put(QueryParams.STATUS_NAME.key(), parameters.get(QueryParams.STATUS_NAME.key()));
            document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }
        if (parameters.containsKey(QueryParams.RELATED_FILES.key())) {
            Object o = parameters.get(QueryParams.RELATED_FILES.key());
            if (o instanceof List<?>) {
                List<Document> relatedFiles = new ArrayList<>(((List<?>) o).size());
                for (Object relatedFile : ((List<?>) o)) {
                    relatedFiles.add(getMongoDBDocument(relatedFile, "RelatedFile"));
                }
                document.getSet().put(QueryParams.RELATED_FILES.key(), relatedFiles);
            }
        }
        if (parameters.containsKey(QueryParams.INDEX_TRANSFORMED_FILE.key())) {
            document.getSet().put(QueryParams.INDEX_TRANSFORMED_FILE.key(),
                    getMongoDBDocument(parameters.get(QueryParams.INDEX_TRANSFORMED_FILE.key()), "TransformedFile"));
        }

        String[] acceptedLongParams = {QueryParams.SIZE.key(), QueryParams.JOB_UID.key()};
        filterLongParams(parameters, document.getSet(), acceptedLongParams);

        // Check if the job exists.
        if (parameters.containsKey(QueryParams.JOB_UID.key()) && parameters.getLong(QueryParams.JOB_UID.key()) > 0) {
            if (!this.dbAdaptorFactory.getCatalogJobDBAdaptor().exists(parameters.getLong(QueryParams.JOB_UID.key()))) {
                throw CatalogDBException.uidNotFound("Job", parameters.getLong(QueryParams.JOB_UID.key()));
            }
        }

        // Check if the samples exist.
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            List<Object> objectSampleList = parameters.getAsList(QueryParams.SAMPLES.key());
            List<Sample> sampleList = new ArrayList<>();
            for (Object sample : objectSampleList) {
                if (sample instanceof Sample) {
                    if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().exists(((Sample) sample).getUid())) {
                        throw CatalogDBException.uidNotFound("Sample", ((Sample) sample).getUid());
                    }
                    sampleList.add((Sample) sample);
                }
            }

            if (!sampleList.isEmpty()) {
                Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
                String operation = (String) actionMap.getOrDefault(QueryParams.SAMPLES.key(), "ADD");
                switch (operation) {
                    case "SET":
                        document.getSet().put(QueryParams.SAMPLES.key(), fileConverter.convertSamples(sampleList));
                        break;
                    case "REMOVE":
                        document.getPullAll().put(QueryParams.SAMPLES.key(), fileConverter.convertSamples(sampleList));
                        break;
                    case "ADD":
                    default:
                        document.getAddToSet().put(QueryParams.SAMPLES.key(), fileConverter.convertSamples(sampleList));
                        break;
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);
        // Fixme: Attributes and stats can be also parsed to numeric or boolean

        String[] acceptedObjectParams = {QueryParams.INDEX.key(), QueryParams.SOFTWARE.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (!document.toFinalUpdateDocument().isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);

            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        return document;
    }

    @Override
    public void delete(long id) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), id);
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
    public QueryResult<File> rename(long fileUid, String filePath, String fileUri, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        checkId(fileUid);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        Document fileDoc = (Document) nativeGet(new Query(QueryParams.UID.key(), fileUid), null).getResult().get(0);
        File file = fileConverter.convertToDataModelType(fileDoc, options);

        if (file.getType().equals(File.Type.DIRECTORY)) {
            filePath += filePath.endsWith("/") ? "" : "/";
        }

        long studyId = (long) fileDoc.get(PRIVATE_STUDY_ID);
        long collisionFileId = getId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.DIRECTORY)) {  // recursive over the files inside folder
            QueryResult<File> allFilesInFolder = getAllFilesInFolder(studyId, file.getPath(), null);
            String oldPath = file.getPath();
            URI uri = file.getUri();
            String oldUri = uri != null ? uri.toString() : "";
            for (File subFile : allFilesInFolder.getResult()) {
                String replacedPath = subFile.getPath().replaceFirst(oldPath, filePath);
                String replacedUri = subFile.getUri().toString().replaceFirst(oldUri, fileUri);
                rename(subFile.getUid(), replacedPath, replacedUri, null); // first part of the path in the subfiles 3
            }
        }

        String fileId = StringUtils.replace(filePath, "/", ":");

        Document query = new Document(PRIVATE_UID, fileUid);
        Document set = new Document("$set", new Document()
                .append(QueryParams.ID.key(), fileId)
                .append(QueryParams.NAME.key(), fileName)
                .append(QueryParams.PATH.key(), filePath)
                .append(QueryParams.URI.key(), fileUri));
        QueryResult<UpdateResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.uidNotFound("File", fileUid);
        }
        return endQuery("Rename file", startTime, get(fileUid, options));
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        return endQuery("Restore files", startTime, setStatus(query, File.FileStatus.READY));
    }

    @Override
    public QueryResult<File> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The file {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, File.FileStatus.READY);
        query = new Query(QueryParams.UID.key(), id);

        return endQuery("Restore file", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query, false);
        return fileCollection.count(bson);
    }

    @Override
    public QueryResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_FILES : studyPermissions);

           // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getFilePermission().name());
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        logger.debug("File count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
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
    public QueryResult<File> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<File> documentList = new ArrayList<>();
        QueryResult<File> queryResult;
        try (DBIterator<File> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult<File> get(long fileId, QueryOptions options) throws CatalogDBException {
        checkId(fileId);
        Query query = new Query()
                .append(QueryParams.UID.key(), fileId)
                .append(QueryParams.STUDY_UID.key(), getStudyIdByFileId(fileId));
        return get(query, options);
    }

    @Override
    public QueryResult<File> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<File> documentList = new ArrayList<>();
        QueryResult<File> queryResult;
        try (DBIterator<File> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_FILES);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        QueryResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery("Native get", startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            QueryResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<File> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new FileMongoDBIterator<>(mongoCursor, fileConverter, null, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new FileMongoDBIterator<>(mongoCursor, null, null, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), null, queryOptions);
    }

    @Override
    public DBIterator<File> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);

        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileMongoDBIterator<>(mongoCursor, fileConverter, iteratorFilter, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), user, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions, studyDocument, user);

        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileMongoDBIterator<>(mongoCursor, null, iteratorFilter, dbAdaptorFactory.getCatalogSampleDBAdaptor(),
                query.getLong(PRIVATE_STUDY_ID), user, options);
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> documentMongoCursor;
        try {
            documentMongoCursor = getMongoCursor(query, options, null, null);
        } catch (CatalogAuthorizationException e) {
            throw new CatalogDBException(e);
        }
        return documentMongoCursor;
    }

    private MongoCursor<Document> getMongoCursor(Query query, QueryOptions options, Document studyDocument, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document queryForAuthorisedEntries = null;
        if (studyDocument != null && user != null) {
            // Get the document query needed to check the permissions as well
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILES.name(), FileAclEntry.FilePermissions.VIEW.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, false, queryForAuthorisedEntries);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = removeInnerProjections(qOptions, QueryParams.SAMPLES.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);
        qOptions = filterOptions(qOptions, FILTER_ROUTE_FILES);

        logger.debug("File query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return fileCollection.nativeQuery().find(bson, qOptions).iterator();
    }

    private void filterOutDeleted(Query query) {
        if (!query.containsKey(QueryParams.STATUS_NAME.key()) && !query.containsKey(QueryParams.UID.key())) {
            query.append(QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + Status.DELETED + ";!="
                    + File.FileStatus.REMOVED + ";!=" + File.FileStatus.PENDING_DELETE + ";!=" + File.FileStatus.DELETING);
        }
    }

    private Document getStudyDocument(Query query) throws CatalogDBException {
        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        QueryResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }
        return queryResult.first();
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return rank(fileCollection, bsonQuery, field, QueryParams.NAME.key(), numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(fileCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false);
        return groupBy(fileCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                    FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILES.name(), FileAclEntry.FilePermissions.VIEW.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(fileCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                    FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILES.name(), FileAclEntry.FilePermissions.VIEW.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, false, queryForAuthorisedEntries);
        return groupBy(fileCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        try (DBIterator<File> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    // Auxiliar methods

    private Bson parseQuery(Query query, boolean isolated) throws CatalogDBException {
        return parseQuery(query, isolated, null);
    }

    protected Bson parseQuery(Query query, boolean isolated, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        if (isolated) {
            andBsonList.add(new Document("$isolated", 1));
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), query);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), query);

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.PRIVATE_ANNOTATION_PARAM_TYPES.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case DIRECTORY:
                        // We add the regex in order to look for all the files under the given directory
                        String value = (String) query.get(queryParam.key());
                        String regExPath = "~^" + value + "[^/]+/?$";
                        Query pathQuery = new Query(QueryParams.PATH.key(), regExPath);
                        addAutoOrQuery(QueryParams.PATH.key(), QueryParams.PATH.key(), pathQuery, QueryParams.PATH.type(), andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(query.getString(QueryParams.ANNOTATION.key()),
                                    query.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case NAME:
                    case UUID:
                    case TYPE:
                    case FORMAT:
                    case BIOFORMAT:
                    case CHECKSUM:
                    case URI:
                    case PATH:
                    case MODIFICATION_DATE:
                    case DESCRIPTION:
                    case EXTERNAL:
                    case RELEASE:
                    case TAGS:
                    case STATUS:
                    case STATUS_NAME:
                    case STATUS_MSG:
                    case STATUS_DATE:
                    case RELATED_FILES:
                    case RELATED_FILES_RELATION:
                    case SIZE:
                    case EXPERIMENT_UID:
                    case SOFTWARE_NAME:
                    case SOFTWARE_VERSION:
                    case SOFTWARE_COMMIT:
                    case SAMPLE_UIDS:
                    case JOB_UID:
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
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (annotationDocument != null && !annotationDocument.isEmpty()) {
            andBsonList.add(annotationDocument);
        }
        if (authorisation != null && authorisation.size() > 0) {
            andBsonList.add(authorisation);
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

    QueryResult<File> setStatus(long fileId, String status) throws CatalogDBException {
        return update(fileId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    @Override
    public QueryResult<Long> extractSampleFromFiles(Query query, List<Long> sampleIds) throws CatalogDBException {
        long startTime = startQuery();
        Bson bsonQuery = parseQuery(query, true);
        Bson update = new Document("$pull", new Document(QueryParams.SAMPLES.key(), new Document(PRIVATE_UID,
                new Document("$in", sampleIds))));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> updateQueryResult = fileCollection.update(bsonQuery, update, multi);
        return endQuery("Extract samples from files", startTime, Collections.singletonList(updateQueryResult.first().getModifiedCount()));
    }

    @Override
    public void addSamplesToFile(long fileId, List<Sample> samples) throws CatalogDBException {
        if (samples == null || samples.size() == 0) {
            return;
        }
        List<Document> sampleList = fileConverter.convertSamples(samples);
        Bson update = Updates.addEachToSet(QueryParams.SAMPLES.key(), sampleList);
        fileCollection.update(Filters.eq(PRIVATE_UID, fileId), update, QueryOptions.empty());
    }

    @Override
    public void unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        unmarkPermissionRule(fileCollection, studyId, permissionRuleId);
    }
}
