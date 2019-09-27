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
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
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

    public static final String REVERSE_NAME = "_reverse";

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
    public DataResult nativeInsert(Map<String, Object> file, String userId) throws CatalogDBException {
        Document fileDocument = getMongoDBDocument(file, "sample");
        return fileCollection.insert(fileDocument, null);
    }

    @Override
    public DataResult insert(long studyId, File file, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException {
        return runTransaction(
                (clientSession) -> {
                    long tmpStartTime = startQuery();
                    logger.debug("Starting file insert transaction for file id '{}'", file.getId());

                    dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                    insert(clientSession, studyId, file, variableSetList);
                    return endWrite(tmpStartTime, 1, 1, 0, 0, null);
                },
                (e) -> logger.error("Could not create file {}: {}", file.getId(), e.getMessage()));
    }

    long insert(ClientSession clientSession, long studyId, File file, List<VariableSet> variableSetList) throws CatalogDBException {
        if (filePathExists(clientSession, studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath());
        }

        // First we check if we need to create any samples and update current list of samples with the ones created
        if (file.getSamples() != null && !file.getSamples().isEmpty()) {
            List<Sample> sampleList = new ArrayList<>(file.getSamples().size());
            for (Sample sample : file.getSamples()) {
                if (sample.getUid() <= 0) {
                    logger.debug("Sample '{}' needs to be created. Inserting sample...", sample.getId());
                    // Sample needs to be created
                    Sample newSample = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(clientSession, studyId, sample,
                            variableSetList);
                    sampleList.add(newSample);
                } else {
                    logger.debug("Sample '{}' was already registered. No need to create it.", sample.getId());
                    sampleList.add(sample);
                }
                file.setSamples(sampleList);
            }
        }


        //new file uid
        long fileUid = getNewUid(clientSession);
        file.setUid(fileUid);
        file.setStudyUid(studyId);
        if (StringUtils.isEmpty(file.getUuid())) {
            file.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
        }
        if (StringUtils.isEmpty(file.getCreationDate())) {
            file.setCreationDate(TimeUtils.getTime());
        }

        Document fileDocument = fileConverter.convertToStorageType(file, variableSetList);

        fileDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        fileDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(file.getCreationDate()));

        fileCollection.insert(clientSession, fileDocument, null);

        // Update the size field from the study collection
        if (!file.isExternal() && file.getSize() > 0) {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(clientSession, studyId, file.getSize());
        }

        return fileUid;
    }

    @Override
    public long getId(long studyId, String path) throws CatalogDBException {
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId).append(QueryParams.PATH.key(), path);
        QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, PRIVATE_UID);
        DataResult<File> fileDataResult = get(query, options);
        return fileDataResult.getNumTotalResults() == 1 ? fileDataResult.getResults().get(0).getUid() : -1;
    }

    @Override
    public DataResult<File> getAllInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId);
        return get(query, options);
    }

    @Override
    public DataResult<File> getAllFilesInFolder(long studyId, String path, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = Filters.and(Filters.eq(PRIVATE_STUDY_UID, studyId), Filters.regex("path", "^" + path + "[^/]+/?$"));
        List<File> fileResults = fileCollection.find(query, fileConverter, null).getResults();
        return endQuery(startTime, fileResults);
    }

    @Override
    public long getStudyIdByFileId(long fileId) throws CatalogDBException {
        Query query = new Query()
                .append(QueryParams.UID.key(), fileId)
                .append(QueryParams.STATUS_NAME.key(), "!=null");
        DataResult queryResult = nativeGet(query, null);

        if (!queryResult.getResults().isEmpty()) {
            return (long) ((Document) queryResult.getResults().get(0)).get(PRIVATE_STUDY_UID);
        } else {
            throw CatalogDBException.uidNotFound("File", fileId);
        }
    }

    @Override
    public DataResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName) throws CatalogDBException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        DataResult<File> fileDataResult = get(id, queryOptions);
        if (fileDataResult.first().getAnnotationSets().isEmpty()) {
            return new DataResult<>(fileDataResult.getTime(), fileDataResult.getWarnings(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = fileDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new DataResult<>(fileDataResult.getTime(), fileDataResult.getWarnings(), size, annotationSets, size);
        }
    }

    @Override
    public DataResult update(long id, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        DataResult update = update(new Query(QueryParams.UID.key(), id), parameters, variableSetList, queryOptions);
        if (update.getNumUpdated() != 1) {
            throw new CatalogDBException("Could not update file with id " + id);
        }
        return update;
    }

    @Override
    public DataResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException {
        long startTime = startQuery();

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.SIZE.key(), QueryParams.STUDY_UID.key()));
        DBIterator<File> iterator = iterator(query, options);

        int numMatches = 0;
        int numModified = 0;
        List<String> warnings = new ArrayList<>();

        while (iterator.hasNext()) {
            File file = iterator.next();
            numMatches += 1;

            try {
                runTransaction(clientSession -> {
                    long tmpStartTime = startQuery();

                    Query tmpQuery = new Query()
                            .append(QueryParams.STUDY_UID.key(), file.getStudyUid())
                            .append(QueryParams.UID.key(), file.getUid());

                    // We perform the update.
                    Bson queryBson = parseQuery(tmpQuery);
                    updateAnnotationSets(clientSession, file.getUid(), parameters, variableSetList, queryOptions, false);

                    Document updateDocument = getValidatedUpdateParams(clientSession, parameters, queryOptions).toFinalUpdateDocument();

                    if (!updateDocument.isEmpty()) {
                        logger.debug("Update file. Query: {}, Update: {}",
                                queryBson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

                        DataResult result = fileCollection.update(clientSession, queryBson, updateDocument,
                                new QueryOptions("multi", true));

                        // If the size of some of the files have been changed, notify to the correspondent study
                        if (parameters.containsKey(QueryParams.SIZE.key())) {
                            long newDiskUsage = parameters.getLong(QueryParams.SIZE.key());
                            long difDiskUsage = newDiskUsage - file.getSize();
                            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(clientSession, file.getStudyUid(), difDiskUsage);
                        }
                        return endWrite(startTime, (int) result.getNumUpdated(), (int) result.getNumUpdated(), null);
                    }

                    return endWrite(tmpStartTime, 1, 1, null);
                });
                logger.info("File {} successfully updated", file.getId());
                numModified += 1;
            } catch (CatalogDBException e) {
                String errorMsg = "Could not update file " + file.getId() + ": " + e.getMessage();
                logger.error("{}", errorMsg);
                warnings.add(errorMsg);
            }
        }

        return endWrite(startTime, numMatches, numModified, warnings);
    }

    @Override
    public DataResult update(long id, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public DataResult update(Query query, ObjectMap parameters, QueryOptions queryOptions) throws CatalogDBException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    private UpdateDocument getValidatedUpdateParams(ClientSession clientSession, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(), QueryParams.PATH.key(),
                QueryParams.CHECKSUM.key(),
        };
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (parameters.containsKey(QueryParams.PATH.key())) {
            // Check the path is not in use
            Query query = new Query(QueryParams.PATH.key(), parameters.getString(QueryParams.PATH.key()));
            if (count(clientSession, query).first() > 0) {
                throw new CatalogDBException("Path " + parameters.getString(QueryParams.PATH.key()) + " already in use");
            }

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
            List<File.RelatedFile> relatedFiles = parameters.getAsList(QueryParams.RELATED_FILES.key(), File.RelatedFile.class);
            List<Document> relatedFileDocument = fileConverter.convertRelatedFiles(relatedFiles);
            document.getSet().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
        }
        if (parameters.containsKey(QueryParams.INDEX_TRANSFORMED_FILE.key())) {
            document.getSet().put(QueryParams.INDEX_TRANSFORMED_FILE.key(),
                    getMongoDBDocument(parameters.get(QueryParams.INDEX_TRANSFORMED_FILE.key()), "TransformedFile"));
        }

        String[] acceptedLongParams = {QueryParams.SIZE.key(), QueryParams.JOB_UID.key()};
        filterLongParams(parameters, document.getSet(), acceptedLongParams);

        // Check if the job exists.
        if (parameters.containsKey(QueryParams.JOB_UID.key()) && parameters.getLong(QueryParams.JOB_UID.key()) > 0) {
            if (!this.dbAdaptorFactory.getCatalogJobDBAdaptor().exists(clientSession, parameters.getLong(QueryParams.JOB_UID.key()))) {
                throw CatalogDBException.uidNotFound("Job", parameters.getLong(QueryParams.JOB_UID.key()));
            }
        }

        // Check if the samples exist.
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            List<Object> objectSampleList = parameters.getAsList(QueryParams.SAMPLES.key());
            List<Sample> sampleList = new ArrayList<>();
            for (Object sample : objectSampleList) {
                if (sample instanceof Sample) {
                    if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().exists(clientSession, ((Sample) sample).getUid())) {
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
    public DataResult delete(File file) throws CatalogDBException {
        throw new UnsupportedOperationException("Use delete passing status field.");
    }

    @Override
    public DataResult delete(Query query) throws CatalogDBException {
        throw new UnsupportedOperationException("Use delete passing status field.");
    }

    @Override
    public DataResult delete(long fileUid, String status) throws CatalogDBException {
        Query query = new Query(QueryParams.UID.key(), fileUid);
        DataResult delete = delete(query, status);
        if (delete.getNumMatches() == 0) {
            throw new CatalogDBException("Could not delete file. Uid " + fileUid + " not found.");
        } else if (delete.getNumUpdated() == 0) {
            throw new CatalogDBException("Could not delete file.");
        }
        return delete;
    }

    @Override
    public DataResult delete(Query query, String status) throws CatalogDBException {
        switch (status) {
            case File.FileStatus.TRASHED:
            case File.FileStatus.REMOVED:
            case File.FileStatus.PENDING_DELETE:
            case File.FileStatus.DELETING:
            case File.FileStatus.DELETED:
                break;
            default:
                throw new CatalogDBException("Invalid status '" + status + "' for deletion of file.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.PATH.key(), QueryParams.UID.key(), QueryParams.EXTERNAL.key(),
                        QueryParams.STATUS.key(), QueryParams.STUDY_UID.key(), QueryParams.TYPE.key()));
        DBIterator<File> iterator = iterator(query, options);

        long startTime = startQuery();
        int numMatches = 0;
        int numModified = 0;
        List<String> warnings = new ArrayList<>();

        while (iterator.hasNext()) {
            File file = iterator.next();
            numMatches += 1;

            try {
                runTransaction(clientSession -> {
                    long tmpStartTime = startQuery();

                    logger.info("Deleting file {} ({})", file.getPath(), file.getUid());

                    dbAdaptorFactory.getCatalogJobDBAdaptor().removeFileReferences(clientSession, file.getStudyUid(), file.getUid(),
                            fileConverter.convertToStorageType(file, null));

                    String deleteSuffix = "";
                    if (File.FileStatus.PENDING_DELETE.equals(status)) {
                        deleteSuffix = INTERNAL_DELIMITER + File.FileStatus.DELETED + "_" + TimeUtils.getTime();
                    } else if (File.FileStatus.REMOVED.equals(status)) {
                        deleteSuffix = INTERNAL_DELIMITER + File.FileStatus.REMOVED + "_" + TimeUtils.getTime();
                    }

                    Query fileQuery = new Query()
                            .append(QueryParams.UID.key(), file.getUid())
                            .append(QueryParams.STUDY_UID.key(), file.getStudyUid());
                    // Mark the file as deleted
                    UpdateDocument document = new UpdateDocument();
                    document.getSet().put(QueryParams.STATUS_NAME.key(), status);
                    document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
                    document.getSet().put(QueryParams.ID.key(), file.getId() + deleteSuffix);
                    if (file.getType() == File.Type.DIRECTORY && file.getPath().endsWith("/")) {
                        // Remove the last /
                        document.getSet().put(QueryParams.PATH.key(), file.getPath().substring(0, file.getPath().length() - 1)
                                + deleteSuffix);
                    } else {
                        document.getSet().put(QueryParams.PATH.key(), file.getPath() + deleteSuffix);
                    }

                    Bson bsonQuery = parseQuery(fileQuery);
                    Document updateDocument = document.toFinalUpdateDocument();

                    logger.debug("Delete file '{}': Query: {}, update: {}", file.getPath(),
                            bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                            updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
                    DataResult result = fileCollection.update(clientSession, bsonQuery, updateDocument, QueryOptions.empty());
                    if (result.getNumUpdated() == 1) {
                        logger.info("File {}({}) deleted", file.getPath(), file.getUid());
                    } else {
                        logger.info("File {}({}) could not be deleted", file.getPath(), file.getUid());
                    }

                    return endWrite(tmpStartTime, 1, 1, null);
                });
                logger.info("File {} successfully deleted", file.getId());
                numModified += 1;
            } catch (Exception e) {
                String errorMsg = "Could not delete file " + file.getId() + ": " + e.getMessage();
                logger.error("{}", errorMsg);
                warnings.add(errorMsg);
            }
        }

        return endWrite(startTime, numMatches, numModified, warnings);
    }

    @Override
    public DataResult rename(long fileUid, String filePath, String fileUri, QueryOptions options) throws CatalogDBException {
        checkId(fileUid);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        Document fileDoc = (Document) nativeGet(new Query(QueryParams.UID.key(), fileUid), null).getResults().get(0);
        File file = fileConverter.convertToDataModelType(fileDoc, options);

        if (file.getType().equals(File.Type.DIRECTORY)) {
            filePath += filePath.endsWith("/") ? "" : "/";
        }

        long studyId = (long) fileDoc.get(PRIVATE_STUDY_UID);
        long collisionFileId = getId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.DIRECTORY)) {  // recursive over the files inside folder
            DataResult<File> allFilesInFolder = getAllFilesInFolder(studyId, file.getPath(), null);
            String oldPath = file.getPath();
            URI uri = file.getUri();
            String oldUri = uri != null ? uri.toString() : "";
            for (File subFile : allFilesInFolder.getResults()) {
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
                .append(REVERSE_NAME, StringUtils.reverse(fileName))
                .append(QueryParams.PATH.key(), filePath)
                .append(QueryParams.URI.key(), fileUri));
        DataResult result = fileCollection.update(query, set, null);
        if (result.getNumUpdated() == 0) {
            throw CatalogDBException.uidNotFound("File", fileUid);
        }
        return result;
    }

    @Override
    public DataResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        query.put(QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        return setStatus(query, File.FileStatus.READY);
    }

    @Override
    public DataResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        checkId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.UID.key(), id)
                .append(QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The file {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        return setStatus(id, File.FileStatus.READY);
    }

    @Override
    public DataResult<Long> count(Query query) throws CatalogDBException {
        return count(null, query);
    }

    DataResult<Long> count(ClientSession clientSession, Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return fileCollection.count(clientSession, bson);
    }

    @Override
    public DataResult<Long> count(final Query query, final String user, final StudyAclEntry.StudyPermissions studyPermissions)
            throws CatalogDBException, CatalogAuthorizationException {
        filterOutDeleted(query);

        StudyAclEntry.StudyPermissions studyPermission = (studyPermissions == null
                ? StudyAclEntry.StudyPermissions.VIEW_FILES : studyPermissions);

        // Get the study document
        Query studyQuery = new Query(StudyDBAdaptor.QueryParams.UID.key(), query.getLong(QueryParams.STUDY_UID.key()));
        DataResult queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }

        // Get the document query needed to check the permissions as well
        Document queryForAuthorisedEntries = getQueryForAuthorisedEntries((Document) queryResult.first(), user,
                studyPermission.name(), studyPermission.getFilePermission().name(), Entity.FILE.name());
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
        logger.debug("File count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return fileCollection.count(bson);
    }

    @Override
    public DataResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return fileCollection.distinct(field, bsonDocument);
    }

    @Override
    public DataResult stats(Query query) {
        return null;
    }

    @Override
    public DataResult<File> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<File> documentList = new ArrayList<>();
        DataResult<File> queryResult;
        try (DBIterator<File> dbIterator = iterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult<File> get(long fileId, QueryOptions options) throws CatalogDBException {
        checkId(fileId);
        Query query = new Query()
                .append(QueryParams.UID.key(), fileId)
                .append(QueryParams.STUDY_UID.key(), getStudyIdByFileId(fileId));
        return get(query, options);
    }

    @Override
    public DataResult<File> get(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<File> documentList = new ArrayList<>();
        DataResult<File> queryResult;
        try (DBIterator<File> dbIterator = iterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(query, user, StudyAclEntry.StudyPermissions.VIEW_FILES);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        DataResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DataResult nativeGet(Query query, QueryOptions options, String user) throws CatalogDBException, CatalogAuthorizationException {
        long startTime = startQuery();
        List<Document> documentList = new ArrayList<>();
        DataResult<Document> queryResult;
        try (DBIterator<Document> dbIterator = nativeIterator(query, options, user)) {
            while (dbIterator.hasNext()) {
                documentList.add(dbIterator.next());
            }
        }
        queryResult = endQuery(startTime, documentList);

        if (options != null && options.getBoolean(QueryOptions.SKIP_COUNT, false)) {
            return queryResult;
        }

        // We only count the total number of results if the actual number of results equals the limit established for performance purposes.
        if (options != null && options.getInt(QueryOptions.LIMIT, 0) == queryResult.getNumResults()) {
            DataResult<Long> count = count(query);
            queryResult.setNumTotalResults(count.first());
        }
        return queryResult;
    }

    @Override
    public DBIterator<File> iterator(Query query, QueryOptions options) throws CatalogDBException {
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options);
        return new FileMongoDBIterator<>(mongoCursor, fileConverter, null, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), query.getLong(PRIVATE_STUDY_UID), null, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoCursor<Document> mongoCursor = getMongoCursor(query, queryOptions);
        return new FileMongoDBIterator<>(mongoCursor, null, null, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), query.getLong(PRIVATE_STUDY_UID), null, queryOptions);
    }

    @Override
    public DBIterator<File> iterator(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        MongoCursor<Document> mongoCursor = getMongoCursor(query, options, studyDocument, user);

        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileMongoDBIterator<>(mongoCursor, fileConverter, iteratorFilter, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), query.getLong(PRIVATE_STUDY_UID), user, options);
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

        return new FileMongoDBIterator<>(mongoCursor, null, iteratorFilter, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), query.getLong(PRIVATE_STUDY_UID), user, options);
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
                    StudyAclEntry.StudyPermissions.VIEW_FILES.name(), FileAclEntry.FilePermissions.VIEW.name(), Entity.FILE.name());
        }

        filterOutDeleted(query);
        Bson bson = parseQuery(query, queryForAuthorisedEntries);
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
        DataResult<Document> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().nativeGet(studyQuery, QueryOptions.empty());
        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Study " + query.getLong(QueryParams.STUDY_UID.key()) + " not found");
        }
        return queryResult.first();
    }

    @Override
    public DataResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return rank(fileCollection, bsonQuery, field, QueryParams.NAME.key(), numResults, asc);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                    FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name(), Entity.FILE.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILES.name(), FileAclEntry.FilePermissions.VIEW.name(), Entity.FILE.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
        return groupBy(fileCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
    }

    @Override
    public DataResult groupBy(Query query, String field, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException {
        Document studyDocument = getStudyDocument(query);
        Document queryForAuthorisedEntries;
        if (containsAnnotationQuery(query)) {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                    FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name(), Entity.FILE.name());
        } else {
            queryForAuthorisedEntries = getQueryForAuthorisedEntries(studyDocument, user,
                    StudyAclEntry.StudyPermissions.VIEW_FILES.name(), FileAclEntry.FilePermissions.VIEW.name(), Entity.FILE.name());
        }
        filterOutDeleted(query);
        Bson bsonQuery = parseQuery(query, queryForAuthorisedEntries);
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

    private Bson parseQuery(Query query) throws CatalogDBException {
        return parseQuery(query, null);
    }

    protected Bson parseQuery(Query query, Document authorisation) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        Query myQuery = new Query(query);

        // If we receive a query by format or bioformat and the user is also trying to filter by type=FILE, we will remove the latter
        // to avoid complexity to mongo database as the results obtained should be the same with or without this latter filter
        if ((myQuery.containsKey(QueryParams.FORMAT.key()) || myQuery.containsKey(QueryParams.BIOFORMAT.key()))
                && File.Type.FILE.name().equals(myQuery.get(QueryParams.TYPE.key()))) {
            myQuery.remove(QueryParams.TYPE.key());
        }

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), myQuery);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), myQuery);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), myQuery);

        for (Map.Entry<String, Object> entry : myQuery.entrySet()) {
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
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case DIRECTORY:
                        // We add the regex in order to look for all the files under the given directory
                        String value = (String) myQuery.get(queryParam.key());
                        String regExPath = "~^" + value + "[^/]+/?$";
                        Query pathQuery = new Query(QueryParams.PATH.key(), regExPath);
                        addAutoOrQuery(QueryParams.PATH.key(), QueryParams.PATH.key(), pathQuery, QueryParams.PATH.type(), andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(myQuery.getString(QueryParams.ANNOTATION.key()),
                                    myQuery.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case STATUS_NAME:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                Status.getPositiveStatus(File.FileStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case INDEX_STATUS_NAME:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                Status.getPositiveStatus(FileIndex.IndexStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case NAME:
                        String name = myQuery.getString(queryParam.key());
                        if (name.startsWith("~") && name.endsWith("$")) {
                            // We remove ~ and $
                            name = name.substring(1, name.length() - 1);
                            // We store the name value reversed
                            myQuery.put(queryParam.key(), "~^" + StringUtils.reverse(name));
                            // We look for the name field in the REVERSE db field
                            addAutoOrQuery(REVERSE_NAME, queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        } else {
                            addAutoOrQuery(queryParam.key(), queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        }
                        break;
                    case FORMAT:
                    case BIOFORMAT:
                        // Replace the value for an uppercase string as we know it will always be in that way
                        String uppercaseValue = myQuery.getString(queryParam.key()).toUpperCase();
                        myQuery.put(queryParam.key(), uppercaseValue);
                        addAutoOrQuery(queryParam.key(), queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case UUID:
                    case TYPE:
                    case CHECKSUM:
                    case URI:
                    case PATH:
                    case MODIFICATION_DATE:
                    case DESCRIPTION:
                    case EXTERNAL:
                    case RELEASE:
                    case TAGS:
                    case STATUS:
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
                    case INDEX_STATUS_MESSAGE:
                    case INDEX_JOB_ID:
                    case INDEX_TRANSFORMED_FILE:
                    case STATS:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), myQuery, queryParam.type(), andBsonList);
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

    private boolean filePathExists(ClientSession clientSession, long studyId, String path) {
        Document query = new Document(PRIVATE_STUDY_UID, studyId).append(QueryParams.PATH.key(), path);
        DataResult<Long> count = fileCollection.count(clientSession, query);
        return count.getResults().get(0) != 0;
    }

    DataResult setStatus(long fileId, String status) throws CatalogDBException {
        return update(fileId, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    DataResult setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_NAME.key(), status), QueryOptions.empty());
    }

    @Override
    public DataResult addSamplesToFile(long fileId, List<Sample> samples) throws CatalogDBException {
        if (samples == null || samples.size() == 0) {
            throw new CatalogDBException("No samples passed.");
        }
        List<Document> sampleList = fileConverter.convertSamples(samples);
        Bson update = Updates.addEachToSet(QueryParams.SAMPLES.key(), sampleList);
        return fileCollection.update(Filters.eq(PRIVATE_UID, fileId), update, QueryOptions.empty());
    }

    @Override
    public DataResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(fileCollection, studyId, permissionRuleId);
    }

    void removeSampleReferences(ClientSession clientSession, long studyUid, long sampleUid) throws CatalogDBException {
        Document bsonQuery = new Document()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.SAMPLE_UIDS.key(), sampleUid);

        ObjectMap params = new ObjectMap()
                .append(QueryParams.SAMPLES.key(), Collections.singletonList(new Sample().setUid(sampleUid)));
        // Add the the Remove action for the sample provided
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS,
                new ObjectMap(QueryParams.SAMPLES.key(), ParamUtils.UpdateAction.REMOVE.name()));

        Bson update = getValidatedUpdateParams(clientSession, params, queryOptions).toFinalUpdateDocument();

        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);

        logger.debug("Sample references extraction. Query: {}, update: {}",
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = fileCollection.update(clientSession, bsonQuery, update, multi);
        logger.debug("Sample uid '" + sampleUid + "' references removed from " + result.getNumUpdated() + " out of "
                + result.getNumMatches() + " files");
    }
}
