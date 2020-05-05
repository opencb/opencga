/*
 * Copyright 2015-2020 OpenCB
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.FileCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
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
    private final MongoDBCollection deletedFileCollection;
    private FileConverter fileConverter;

    public static final String REVERSE_NAME = "_reverse";

    /***
     * CatalogMongoFileDBAdaptor constructor.
     *
     * @param fileCollection MongoDB connection to the file collection.
     * @param deletedFileCollection MongoDB connection to the file collection containing the deleted documents.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     */
    public FileMongoDBAdaptor(MongoDBCollection fileCollection, MongoDBCollection deletedFileCollection,
                              MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(FileMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.deletedFileCollection = deletedFileCollection;
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
    public OpenCGAResult nativeInsert(Map<String, Object> file, String userId) throws CatalogDBException {
        Document fileDocument = getMongoDBDocument(file, "sample");
        return new OpenCGAResult(fileCollection.insert(fileDocument, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, File file, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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

    long insert(ClientSession clientSession, long studyId, File file, List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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
            file.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
        }
        if (StringUtils.isEmpty(file.getCreationDate())) {
            file.setCreationDate(TimeUtils.getTime());
        }

        Document fileDocument = fileConverter.convertToStorageType(file, variableSetList);

        fileDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        fileDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(file.getCreationDate()));
        fileDocument.put(PRIVATE_MODIFICATION_DATE, fileDocument.get(PRIVATE_CREATION_DATE));

        fileCollection.insert(clientSession, fileDocument, null);

        // Update the size field from the study collection
        if (!file.isExternal() && file.getSize() > 0) {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(clientSession, studyId, file.getSize());
        }

        return fileUid;
    }

    @Override
    public long getId(long studyId, String path) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId).append(QueryParams.PATH.key(), path);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, PRIVATE_UID);
        OpenCGAResult<File> fileDataResult = get(query, options);
        return fileDataResult.getNumResults() == 1 ? fileDataResult.first().getUid() : -1;
    }

    @Override
    public OpenCGAResult<File> getAllInStudy(long studyId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
        Query query = new Query(QueryParams.STUDY_UID.key(), studyId);
        return get(query, options);
    }

    @Override
    public OpenCGAResult<File> getAllFilesInFolder(long studyId, String path, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = Filters.and(Filters.eq(PRIVATE_STUDY_UID, studyId), Filters.regex("path", "^" + path + "[^/]+/?$"));
        DataResult<File> fileResults = fileCollection.find(query, fileConverter, null);
        return endQuery(startTime, fileResults);
    }

    @Override
    public long getStudyIdByFileId(long fileId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), fileId);
        OpenCGAResult queryResult = nativeGet(query, null);

        if (!queryResult.getResults().isEmpty()) {
            return (long) ((Document) queryResult.getResults().get(0)).get(PRIVATE_STUDY_UID);
        } else {
            throw CatalogDBException.uidNotFound("File", fileId);
        }
    }

    @Override
    public OpenCGAResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        OpenCGAResult<File> fileDataResult = get(id, queryOptions);
        if (fileDataResult.first().getAnnotationSets().isEmpty()) {
            return new OpenCGAResult<>(fileDataResult.getTime(), fileDataResult.getEvents(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = fileDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new OpenCGAResult<>(fileDataResult.getTime(), fileDataResult.getEvents(), size, annotationSets, size);
        }
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(id, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(long fileUid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.SIZE.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<File> fileDataResult = get(fileUid, options);

        if (fileDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update file. File uid '" + fileUid + "' not found.");
        }

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, fileDataResult.first(), parameters,
                    variableSetList, queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update file {}: {}", fileDataResult.first().getPath(), e.getMessage(), e);
            throw new CatalogDBException("Could not update file " + fileDataResult.first().getPath() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.SIZE.key(), QueryParams.STUDY_UID.key()));
        DBIterator<File> iterator = iterator(query, options);

        OpenCGAResult<File> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            File file = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, file, parameters, variableSetList,
                        queryOptions)));
            } catch (CatalogDBException e) {
                logger.error("Could not update file {}: {}", file.getPath(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, file.getPath(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, File file, ObjectMap parameters, List<VariableSet> variableSetList,
                                        QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), file.getStudyUid())
                .append(QueryParams.UID.key(), file.getUid());

        // We perform the update.
        Bson queryBson = parseQuery(tmpQuery);
        DataResult result = updateAnnotationSets(clientSession, file.getUid(), parameters, variableSetList, queryOptions, false);

        Document updateDocument = getValidatedUpdateParams(clientSession, parameters, queryOptions).toFinalUpdateDocument();

        if (updateDocument.isEmpty() && result.getNumUpdated() == 0) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        List<Event> events = new ArrayList<>();
        if (!updateDocument.isEmpty()) {
            logger.debug("Update file. Query: {}, Update: {}",
                    queryBson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            result = fileCollection.update(clientSession, queryBson, updateDocument, null);

            // If the size of some of the files have been changed, notify to the correspondent study
            if (parameters.containsKey(QueryParams.SIZE.key())) {
                long newDiskUsage = parameters.getLong(QueryParams.SIZE.key());
                long difDiskUsage = newDiskUsage - file.getSize();
                dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(clientSession, file.getStudyUid(), difDiskUsage);
            }

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("File " + file.getPath() + " not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, file.getPath(), "File was already updated"));
            }
            logger.debug("File {} successfully updated", file.getPath());
        }

        return endWrite(tmpStartTime, 1, 1, events);
    }

    private UpdateDocument getValidatedUpdateParams(ClientSession clientSession, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(), QueryParams.PATH.key(),
                QueryParams.CHECKSUM.key(), QueryParams.JOB_ID.key(),
        };
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (parameters.containsKey(QueryParams.PATH.key())) {
            // Check the path is not in use
            Query query = new Query(QueryParams.PATH.key(), parameters.getString(QueryParams.PATH.key()));
            if (count(clientSession, query).getNumMatches() > 0) {
                throw new CatalogDBException("Path " + parameters.getString(QueryParams.PATH.key()) + " already in use");
            }

            // We also update the ID replacing the / for :
            String path = parameters.getString(QueryParams.PATH.key());
            document.getSet().put(QueryParams.ID.key(), StringUtils.replace(path, "/", ":"));
        }

        // Check if the tags exist.
        if (parameters.containsKey(QueryParams.TAGS.key())) {
            List<String> tagList = parameters.getAsStringList(QueryParams.TAGS.key());

            if (!tagList.isEmpty()) {
                Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
                String operation = (String) actionMap.getOrDefault(QueryParams.TAGS.key(), "ADD");
                switch (operation) {
                    case "SET":
                        document.getSet().put(QueryParams.TAGS.key(), tagList);
                        break;
                    case "REMOVE":
                        document.getPullAll().put(QueryParams.TAGS.key(), tagList);
                        break;
                    case "ADD":
                    default:
                        document.getAddToSet().put(QueryParams.TAGS.key(), tagList);
                        break;
                }
            }
        }

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

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_NAME.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_NAME.key(), parameters.get(QueryParams.INTERNAL_STATUS_NAME.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.RELATED_FILES.key())) {
            List<FileRelatedFile> relatedFiles = parameters.getAsList(QueryParams.RELATED_FILES.key(), FileRelatedFile.class);
            List<Document> relatedFileDocument = fileConverter.convertRelatedFiles(relatedFiles);

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            String operation = (String) actionMap.getOrDefault(QueryParams.RELATED_FILES.key(), "ADD");

            switch (operation) {
                case "SET":
                    document.getSet().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
                case "REMOVE":
                    document.getPullAll().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
                case "ADD":
                default:
                    document.getAddToSet().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
            }
        }
        if (parameters.containsKey(QueryParams.INTERNAL_INDEX_TRANSFORMED_FILE.key())) {
            document.getSet().put(QueryParams.INTERNAL_INDEX_TRANSFORMED_FILE.key(),
                    getMongoDBDocument(parameters.get(QueryParams.INTERNAL_INDEX_TRANSFORMED_FILE.key()), "TransformedFile"));
        }

        String[] acceptedLongParams = {QueryParams.SIZE.key()};
        filterLongParams(parameters, document.getSet(), acceptedLongParams);

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

        String[] acceptedObjectParams = {QueryParams.INTERNAL_INDEX.key(), QueryParams.SOFTWARE.key(), QueryParams.EXPERIMENT.key(),
                QueryParams.STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

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
    public OpenCGAResult delete(File file) throws CatalogDBException {
        throw new UnsupportedOperationException("Use delete passing status field.");
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException {
        throw new UnsupportedOperationException("Use delete passing status field.");
    }

    @Override
    public OpenCGAResult delete(File file, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        switch (status) {
            case FileStatus.TRASHED:
            case FileStatus.REMOVED:
//            case File.FileStatus.PENDING_DELETE:
//            case File.FileStatus.DELETING:
            case FileStatus.DELETED:
                break;
            default:
                throw new CatalogDBException("Invalid status '" + status + "' for deletion of file.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.PATH.key(), QueryParams.UID.key(), QueryParams.EXTERNAL.key(),
                        QueryParams.INTERNAL_STATUS.key(), QueryParams.STUDY_UID.key(), QueryParams.TYPE.key()));
        Document fileDocument = nativeGet(new Query(QueryParams.UID.key(), file.getUid()), options).first();

        try {
            return runTransaction(clientSession -> privateDelete(clientSession, fileDocument, status));
        } catch (CatalogDBException e) {
            logger.error("Could not delete file {}: {}", file.getPath(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete file " + file.getPath() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        switch (status) {
            case FileStatus.TRASHED:
            case FileStatus.REMOVED:
//            case File.FileStatus.PENDING_DELETE:
//            case File.FileStatus.DELETING:
            case FileStatus.DELETED:
                break;
            default:
                throw new CatalogDBException("Invalid status '" + status + "' for deletion of file.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.PATH.key(), QueryParams.UID.key(), QueryParams.EXTERNAL.key(),
                        QueryParams.INTERNAL_STATUS.key(), QueryParams.STUDY_UID.key(), QueryParams.TYPE.key()));
        DBIterator<Document> iterator = nativeIterator(query, options);

        OpenCGAResult<File> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Document fileDocument = iterator.next();
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, fileDocument, status)));
            } catch (CatalogDBException e) {
                logger.error("Could not delete file {}: {}", fileDocument.getString(QueryParams.PATH.key()), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, fileDocument.getString(QueryParams.ID.key()), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document fileDocument, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        long fileUid = fileDocument.getLong(PRIVATE_UID);
        long studyUid = fileDocument.getLong(PRIVATE_STUDY_UID);
        String path = fileDocument.getString(QueryParams.PATH.key());

        Query query = new Query(QueryParams.STUDY_UID.key(), studyUid);
        if (File.Type.FILE.name().equals(fileDocument.getString(QueryParams.TYPE.key()))) {
            query.append(QueryParams.UID.key(), fileUid);
        } else {
            // Look for all the nested files and folders
            query.append(QueryParams.PATH.key(), "~^" + path + "*");
        }

        if (FileStatus.TRASHED.equals(status)) {
            Bson update = Updates.set(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new FileStatus(status), "status"));
            QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
            return endWrite(tmpStartTime, fileCollection.update(parseQuery(query), update, multi));
        } else {
            // DELETED AND REMOVED status
            QueryOptions options = new QueryOptions()
                    .append(QueryOptions.SORT, QueryParams.PATH.key())
                    .append(QueryOptions.ORDER, QueryOptions.DESCENDING);

            DBIterator<Document> iterator = nativeIterator(clientSession, query, options);

            // TODO: Delete any documents that might have been previously deleted under the same paths
            long numFiles = 0;

            while (iterator.hasNext()) {
                Document tmpFile = iterator.next();
                long tmpFileUid = tmpFile.getLong(PRIVATE_UID);

                dbAdaptorFactory.getCatalogJobDBAdaptor().removeFileReferences(clientSession, studyUid, tmpFileUid, tmpFile);

                // Set status
                nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new FileStatus(status), "status"), tmpFile);

                // Insert the document in the DELETE collection
                deletedFileCollection.insert(clientSession, tmpFile, null);
                logger.debug("Inserted file uid '{}' in DELETE collection", tmpFileUid);

                // Remove the document from the main FILE collection
                Bson bsonQuery = parseQuery(new Query(QueryParams.UID.key(), tmpFileUid));
                DataResult remove = fileCollection.remove(clientSession, bsonQuery, null);
                if (remove.getNumMatches() == 0) {
                    throw new CatalogDBException("File " + tmpFileUid + " not found");
                }
                if (remove.getNumDeleted() == 0) {
                    throw new CatalogDBException("File " + tmpFileUid + " could not be deleted");
                }

                logger.debug("File uid '{}' deleted from main FILE collection", tmpFileUid);
                numFiles++;
            }

            logger.debug("File {}({}) deleted", path, fileUid);
            return endWrite(tmpStartTime, numFiles, 0, 0, numFiles, Collections.emptyList());
        }
    }

//    OpenCGAResult<Object> privateDelete(ClientSession clientSession, File file, String status) throws CatalogDBException {
//        long tmpStartTime = startQuery();
//        logger.debug("Deleting file {} ({})", file.getPath(), file.getUid());
//
//        dbAdaptorFactory.getCatalogJobDBAdaptor().removeFileReferences(clientSession, file.getStudyUid(), file.getUid(),
//                fileConverter.convertToStorageType(file, null));
//
//        String deleteSuffix = "";
//        if (File.FileStatus.PENDING_DELETE.equals(status)) {
//            deleteSuffix = INTERNAL_DELIMITER + File.FileStatus.DELETED + "_" + TimeUtils.getTime();
//        } else if (File.FileStatus.REMOVED.equals(status)) {
//            deleteSuffix = INTERNAL_DELIMITER + File.FileStatus.REMOVED + "_" + TimeUtils.getTime();
//        }
//
//        Query fileQuery = new Query()
//                .append(QueryParams.UID.key(), file.getUid())
//                .append(QueryParams.STUDY_UID.key(), file.getStudyUid());
//        // Mark the file as deleted
//        UpdateDocument document = new UpdateDocument();
//        document.getSet().put(QueryParams.STATUS_NAME.key(), status);
//        document.getSet().put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
//        document.getSet().put(QueryParams.ID.key(), file.getId() + deleteSuffix);
//        if (file.getType() == File.Type.DIRECTORY && file.getPath().endsWith("/")) {
//            // Remove the last /
//            document.getSet().put(QueryParams.PATH.key(), file.getPath().substring(0, file.getPath().length() - 1)
//                    + deleteSuffix);
//        } else {
//            document.getSet().put(QueryParams.PATH.key(), file.getPath() + deleteSuffix);
//        }
//
//        Bson bsonQuery = parseQuery(fileQuery);
//        Document updateDocument = document.toFinalUpdateDocument();
//
//        logger.debug("Delete file '{}': Query: {}, update: {}", file.getPath(),
//                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
//                updateDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
//        DataResult result = fileCollection.update(clientSession, bsonQuery, updateDocument, QueryOptions.empty());
//        if (result.getNumMatches() == 0) {
//            throw new CatalogDBException("File " + file.getId() + " not found");
//        }
//        List<Event> events = new ArrayList<>();
//        if (result.getNumUpdated() == 0) {
//            events.add(new Event(Event.Type.WARNING, file.getId(), "File was already deleted"));
//        }
//        logger.debug("File {} successfully deleted", file.getId());
//
//        return endWrite(tmpStartTime, 1, 0, 0, 1, events);
//    }

    @Override
    public OpenCGAResult rename(long fileUid, String filePath, String fileUri, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(fileUid);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        Document fileDoc = nativeGet(new Query(QueryParams.UID.key(), fileUid), null).getResults().get(0);
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
            OpenCGAResult<File> allFilesInFolder = getAllFilesInFolder(studyId, file.getPath(), null);
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
        return new OpenCGAResult(result);
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(fileCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("File count: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(fileCollection.count(bson));
    }

    @Override
    public OpenCGAResult distinct(Query query, String field)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonDocument = parseQuery(query);
        return new OpenCGAResult(fileCollection.distinct(field, bsonDocument));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<File> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<File> dbIterator = iterator(query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<File> get(long fileId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(fileId);
        Query query = new Query()
                .append(QueryParams.UID.key(), fileId)
                .append(QueryParams.STUDY_UID.key(), getStudyIdByFileId(fileId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<File> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<File> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<File> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options);
        return new FileCatalogMongoDBIterator<>(mongoCursor, null, fileConverter, null, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    private DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new FileCatalogMongoDBIterator<>(mongoCursor, clientSession, null, null, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), queryOptions);
    }

    @Override
    public DBIterator<File> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);

        Document studyDocument = getStudyDocument(null, studyUid);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileCatalogMongoDBIterator<File>(mongoCursor, null, fileConverter, iteratorFilter, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), studyUid, user, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    public DBIterator<Document> nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);

        Document studyDocument = getStudyDocument(clientSession, studyUid);
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS.name(),
                FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileCatalogMongoDBIterator<>(mongoCursor, null, null, iteratorFilter, this,
                dbAdaptorFactory.getCatalogSampleDBAdaptor(), studyUid, user, options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
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
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return fileCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedFileCollection.iterator(clientSession, bson, null, null, qOptions);
        }
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(fileCollection, bsonQuery, field, QueryParams.NAME.key(), numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, field, QueryParams.NAME.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(fileCollection, bsonQuery, fields, QueryParams.NAME.key(), options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<File> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    // Auxiliar methods

    private Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));
            if (containsAnnotationQuery(query)) {
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name(),
                        Enums.Resource.FILE));
            } else {
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FileAclEntry.FilePermissions.VIEW.name(),
                        Enums.Resource.FILE));
            }

            andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.FILE, user));

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query myQuery = new Query(query);
        myQuery.remove(QueryParams.DELETED.key());

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
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                Status.getPositiveStatus(FileStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_NAME.key(), queryParam.key(), myQuery,
                                QueryParams.INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    case INTERNAL_INDEX_STATUS_NAME:
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
                    case ID:
                    case PATH:
                    case MODIFICATION_DATE:
                    case DESCRIPTION:
                    case EXTERNAL:
                    case RELEASE:
                    case TAGS:
                    case INTERNAL_STATUS_DESCRIPTION:
                    case INTERNAL_STATUS_DATE:
                    case RELATED_FILES:
                    case RELATED_FILES_RELATION:
                    case SIZE:
                    case SOFTWARE_NAME:
                    case SOFTWARE_VERSION:
                    case SOFTWARE_COMMIT:
                    case SAMPLE_UIDS:
                    case JOB_ID:
                    case INTERNAL_INDEX:
                    case INTERNAL_INDEX_USER_ID:
                    case INTERNAL_INDEX_CREATION_DATE:
                    case INTERNAL_INDEX_STATUS_MESSAGE:
                    case INTERNAL_INDEX_JOB_ID:
                    case INTERNAL_INDEX_TRANSFORMED_FILE:
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
        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
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
        return count.getNumMatches() != 0;
    }

    @Override
    public OpenCGAResult addSamplesToFile(long fileId, List<Sample> samples) throws CatalogDBException {
        if (samples == null || samples.size() == 0) {
            throw new CatalogDBException("No samples passed.");
        }
        List<Document> sampleList = fileConverter.convertSamples(samples);
        Bson update = Updates.addEachToSet(QueryParams.SAMPLES.key(), sampleList);
        return new OpenCGAResult(fileCollection.update(Filters.eq(PRIVATE_UID, fileId), update, QueryOptions.empty()));
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(fileCollection, studyId, permissionRuleId);
    }

    void removeSampleReferences(ClientSession clientSession, long studyUid, long sampleUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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
