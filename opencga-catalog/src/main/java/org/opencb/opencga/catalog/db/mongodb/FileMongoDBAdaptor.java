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
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.FileCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils.BasicUpdateAction;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
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
    public static final String PRIVATE_SAMPLES = "_samples";

    private int fileSampleLinkThreshold = 5000;

    /***
     * CatalogMongoFileDBAdaptor constructor.
     *
     * @param fileCollection MongoDB connection to the file collection.
     * @param deletedFileCollection MongoDB connection to the file collection containing the deleted documents.
     * @param configuration Configuration file.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     */
    public FileMongoDBAdaptor(MongoDBCollection fileCollection, MongoDBCollection deletedFileCollection, Configuration configuration,
                              MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(FileMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.deletedFileCollection = deletedFileCollection;
        this.fileConverter = new FileConverter();
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
    public OpenCGAResult insert(long studyId, File file, List<Sample> existingSamples, List<Sample> nonExistingSamples,
                                List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(
                (clientSession) -> {
                    long tmpStartTime = startQuery();
                    logger.debug("Starting file insert transaction for file id '{}'", file.getId());

                    dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                    insert(clientSession, studyId, file, existingSamples, nonExistingSamples, variableSetList);
                    return endWrite(tmpStartTime, 1, 1, 0, 0, null);
                },
                (e) -> logger.error("Could not create file {}: {}", file.getId(), e.getMessage()));
    }

    long insert(ClientSession clientSession, long studyId, File file, List<Sample> existingSamples, List<Sample> nonExistingSamples,
                List<VariableSet> variableSetList) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (filePathExists(clientSession, studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath());
        }
        if (existingSamples == null) {
            existingSamples = Collections.emptyList();
        }
        if (nonExistingSamples == null) {
            nonExistingSamples = Collections.emptyList();
        }

        List<Sample> samples = new ArrayList<>(existingSamples.size() + nonExistingSamples.size());
        if (existingSamples.size() + nonExistingSamples.size() < fileSampleLinkThreshold) {
            // First we check if we need to create any samples and update current list of samples with the ones created
            if (file.getSampleIds() != null && !file.getSampleIds().isEmpty()) {
                // ------------ PROCESS NON-EXISTING SAMPLES --------------
                for (Sample sample : nonExistingSamples) {
                    logger.debug("Sample '{}' needs to be created. Inserting sample...", sample.getId());

                    // Sample needs to be created
                    sample.setFileIds(Collections.singletonList(file.getId()));
                    Sample newSample = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(clientSession, studyId, sample, variableSetList);
                    samples.add(newSample);
                }

                // ------------ PROCESS EXISTING SAMPLES --------------
                int batchSize = 1000;
                List<List<Sample>> sampleListList = new ArrayList<>((existingSamples.size() / batchSize) + 1);
                // Create batches
                List<Sample> currentList = null;
                for (int i = 0; i < existingSamples.size(); i++) {
                    if (i % batchSize == 0) {
                        currentList = new ArrayList<>(batchSize);
                        sampleListList.add(currentList);
                    }

                    currentList.add(existingSamples.get(i));
                }

                ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());
                ObjectMap actionMap = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), BasicUpdateAction.ADD.name());
                QueryOptions sampleUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);
                UpdateDocument sampleUpdateDocument = dbAdaptorFactory.getCatalogSampleDBAdaptor()
                        .updateFileReferences(params, sampleUpdateOptions);
                for (List<Sample> sampleList : sampleListList) {
                    logger.debug("Updating list of fileIds in batch of {} samples...", sampleList.size());

                    // Update list of fileIds from sample
                    Query query = new Query()
                            .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                            .append(SampleDBAdaptor.QueryParams.UID.key(),
                                    sampleList.stream().map(Sample::getUid).collect(Collectors.toList()));
                    dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection().update(clientSession,
                            dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null),
                            sampleUpdateDocument.toFinalUpdateDocument(), new QueryOptions("multi", true));

                    // Add sample to sampleList
                    samples.addAll(sampleList);
                }
            }
        } else {
            // We change the internal status of the file
            file.getInternal().setStatus(new FileStatus(FileStatus.MISSING_SAMPLES,
                    nonExistingSamples.size() + existingSamples.size() + " missing samples"));
            file.getInternal().setMissingSamples(new MissingSamples(
                    existingSamples.stream().map(Sample::getId).collect(Collectors.toList()),
                    nonExistingSamples.stream().map(Sample::getId).collect(Collectors.toList())));
        }

        //new file uid
        long fileUid = getNewUid();
        file.setUid(fileUid);
        file.setStudyUid(studyId);
        if (StringUtils.isEmpty(file.getUuid())) {
            file.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
        }

        Document fileDocument = fileConverter.convertToStorageType(file, samples, variableSetList);

        fileDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        fileDocument.put(PRIVATE_CREATION_DATE,
                StringUtils.isNotEmpty(file.getCreationDate()) ? TimeUtils.toDate(file.getCreationDate()) : TimeUtils.getDate());
        fileDocument.put(PRIVATE_MODIFICATION_DATE,
                StringUtils.isNotEmpty(file.getModificationDate()) ? TimeUtils.toDate(file.getModificationDate()) : TimeUtils.getDate());

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

        UpdateDocument updateDocument = getValidatedUpdateParams(clientSession, file.getStudyUid(), parameters, tmpQuery, queryOptions);
        Document fileUpdate = updateDocument.toFinalUpdateDocument();

        if (fileUpdate.isEmpty() && result.getNumUpdated() == 0) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        List<Event> events = new ArrayList<>();
        if (!fileUpdate.isEmpty()) {
            logger.debug("Update file. Query: {}, Update: {}",
                    queryBson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    fileUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            result = fileCollection.update(clientSession, queryBson, fileUpdate, null);

            // If the size of some of the files have been changed, notify to the correspondent study
            if (parameters.containsKey(QueryParams.SIZE.key())) {
                long newDiskUsage = parameters.getLong(QueryParams.SIZE.key());
                long difDiskUsage = newDiskUsage - file.getSize();
                dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(clientSession, file.getStudyUid(), difDiskUsage);
            }

            updateSampleReferences(clientSession, file, updateDocument);

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

    private void updateSampleReferences(ClientSession clientSession, File file, UpdateDocument updateDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (!updateDocument.getAttributes().isEmpty()) {
            ObjectMap addedSamples = (ObjectMap) updateDocument.getAttributes().getMap("ADDED_SAMPLES");
            ObjectMap removedSamples = (ObjectMap) updateDocument.getAttributes().getMap("REMOVED_SAMPLES");
            List<Long> setSamples = updateDocument.getAttributes().getAsLongList("SET_SAMPLES");

            if (setSamples.isEmpty() && (addedSamples == null || addedSamples.isEmpty())
                    && (removedSamples == null || removedSamples.isEmpty())) {
                throw new CatalogDBException("Internal error: Expected a list of added, removed or set samples");
            }

            Bson sampleBsonQuery = null;
            UpdateDocument sampleUpdate = null;
            ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());

            if (!setSamples.isEmpty()) {
                // File id has been modified, so we need to replace old file id with the new one
                String newFileId = updateDocument.getSet().getString(QueryParams.ID.key());
                if (StringUtils.isEmpty(newFileId)) {
                    throw new CatalogDBException("Internal error: Expected new file id");
                }

                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), file.getStudyUid())
                        .append(SampleDBAdaptor.QueryParams.UID.key(), setSamples)
                        .append(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());
                sampleBsonQuery = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null);

                // Replace the id for the new one
                sampleUpdate = new UpdateDocument();
                sampleUpdate.getSet().append(SampleDBAdaptor.QueryParams.FILE_IDS.key() + ".$", newFileId);

                dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection().update(clientSession, sampleBsonQuery,
                        sampleUpdate.toFinalUpdateDocument(), new QueryOptions(MongoDBCollection.MULTI, true));
            }
            if (addedSamples != null && !addedSamples.isEmpty()) {
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), file.getStudyUid())
                        .append(SampleDBAdaptor.QueryParams.UID.key(), addedSamples.getAsLongList(file.getId()));
                sampleBsonQuery = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null);

                ObjectMap actionMap = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), BasicUpdateAction.ADD.name());
                QueryOptions sampleUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);

                sampleUpdate = dbAdaptorFactory.getCatalogSampleDBAdaptor().updateFileReferences(params, sampleUpdateOptions);

                dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection().update(clientSession, sampleBsonQuery,
                        sampleUpdate.toFinalUpdateDocument(), new QueryOptions(MongoDBCollection.MULTI, true));
            }
            if (removedSamples != null && !removedSamples.isEmpty()) {
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), file.getStudyUid())
                        .append(SampleDBAdaptor.QueryParams.UID.key(), removedSamples.getAsLongList(file.getId()));
                sampleBsonQuery = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null);

                ObjectMap actionMap = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), BasicUpdateAction.REMOVE.name());
                QueryOptions sampleUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);

                sampleUpdate = dbAdaptorFactory.getCatalogSampleDBAdaptor().updateFileReferences(params, sampleUpdateOptions);

                dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection().update(clientSession, sampleBsonQuery,
                        sampleUpdate.toFinalUpdateDocument(), new QueryOptions(MongoDBCollection.MULTI, true));
            }
        }
    }

    private void getSampleChanges(Document fileDocument, List<Sample> sampleList, UpdateDocument updateDocument,
                                  BasicUpdateAction operation) {
        String fileId = fileDocument.getString(QueryParams.ID.key());

        Set<Long> currentSampleUidList = new HashSet<>();
        if (fileDocument.get(PRIVATE_SAMPLES) != null) {
            currentSampleUidList = fileDocument.getList(PRIVATE_SAMPLES, Document.class).stream()
                    .map(s -> s.get(QueryParams.UID.key(), Long.class))
                    .collect(Collectors.toSet());
        }

        // The file id has been altered !!!
        if (updateDocument.getSet().containsKey(QueryParams.ID.key())) {
            // The current list of samples need to replace the current fileId
            updateDocument.getAttributes().put("SET_SAMPLES", currentSampleUidList);
        } else if (BasicUpdateAction.SET.equals(operation) || BasicUpdateAction.ADD.equals(operation)) {
            // We will see which of the samples are actually new
            List<Long> samplesToAdd = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (!currentSampleUidList.contains(sample.getUid())) {
                    samplesToAdd.add(sample.getUid());
                }
            }

            if (!samplesToAdd.isEmpty()) {
                updateDocument.getAttributes().put("ADDED_SAMPLES", new ObjectMap(fileId, samplesToAdd));
            }

            if (BasicUpdateAction.SET.equals(operation) && fileDocument.get(PRIVATE_SAMPLES) != null) {
                // We also need to see which samples existed and are not currently in the new list provided by the user to take them out
                Set<Long> newSampleUids = sampleList.stream().map(Sample::getUid).collect(Collectors.toSet());

                List<Long> samplesToRemove = new ArrayList<>();
                for (Document sampleDoc : fileDocument.getList(PRIVATE_SAMPLES, Document.class)) {
                    Long sampleUid = sampleDoc.get(SampleDBAdaptor.QueryParams.UID.key(), Long.class);
                    if (!newSampleUids.contains(sampleUid)) {
                        samplesToRemove.add(sampleUid);
                    }
                }

                if (!samplesToRemove.isEmpty()) {
                    updateDocument.getAttributes().put("REMOVED_SAMPLES", new ObjectMap(fileId, samplesToRemove));
                }
            }
        } else if (BasicUpdateAction.REMOVE.equals(operation)) {
            // We will only store the samples to be removed that are already associated to the individual
            List<Long> samplesToRemove = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (currentSampleUidList.contains(sample.getUid())) {
                    samplesToRemove.add(sample.getUid());
                }
            }

            if (!samplesToRemove.isEmpty()) {
                updateDocument.getAttributes().put("REMOVED_SAMPLES", new ObjectMap(fileId, samplesToRemove));
            }
        }
    }

    private UpdateDocument getValidatedUpdateParams(ClientSession clientSession, long studyUid, ObjectMap parameters, Query query,
                                                    QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.PATH.key(), QueryParams.CHECKSUM.key(),
                QueryParams.JOB_ID.key(),
        };
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        if (parameters.containsKey(QueryParams.PATH.key())) {
            checkOnlyOneFileMatches(clientSession, query);

            // Check the path is not in use
            Query pathQuery = new Query(QueryParams.PATH.key(), parameters.getString(QueryParams.PATH.key()));
            if (count(clientSession, pathQuery).getNumMatches() > 0) {
                throw new CatalogDBException("Path " + parameters.getString(QueryParams.PATH.key()) + " already in use");
            }

            // We also update the ID replacing the / for :
            String path = parameters.getString(QueryParams.PATH.key());
            document.getSet().put(QueryParams.ID.key(), StringUtils.replace(path, "/", ":"));
        }

        // Check if the tags exist.
        if (parameters.containsKey(QueryParams.TAGS.key())) {
            List<String> tagList = parameters.getAsStringList(QueryParams.TAGS.key());

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            BasicUpdateAction operation = BasicUpdateAction.from(actionMap, QueryParams.TAGS.key(), BasicUpdateAction.ADD);
            if (BasicUpdateAction.SET.equals(operation) || !tagList.isEmpty()) {
                switch (operation) {
                    case SET:
                        document.getSet().put(QueryParams.TAGS.key(), tagList);
                        break;
                    case REMOVE:
                        document.getPullAll().put(QueryParams.TAGS.key(), tagList);
                        break;
                    case ADD:
                        document.getAddToSet().put(QueryParams.TAGS.key(), tagList);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown operation " + operation);
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

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_ID.key(), parameters.get(QueryParams.INTERNAL_STATUS_ID.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (parameters.containsKey(QueryParams.RELATED_FILES.key())) {
            List<FileRelatedFile> relatedFiles = parameters.getAsList(QueryParams.RELATED_FILES.key(), FileRelatedFile.class);
            List<Document> relatedFileDocument = fileConverter.convertRelatedFiles(relatedFiles);

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            BasicUpdateAction operation = BasicUpdateAction.from(actionMap, QueryParams.RELATED_FILES.key(), BasicUpdateAction.ADD);
            switch (operation) {
                case SET:
                    document.getSet().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
                case REMOVE:
                    document.getPullAll().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
                case ADD:
                    document.getAddToSet().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation " + operation);
            }
        }
        if (parameters.containsKey(QueryParams.INTERNAL_INDEX_TRANSFORMED_FILE.key())) {
            document.getSet().put(QueryParams.INTERNAL_INDEX_TRANSFORMED_FILE.key(),
                    getMongoDBDocument(parameters.get(QueryParams.INTERNAL_INDEX_TRANSFORMED_FILE.key()), "TransformedFile"));
        }

        String[] acceptedLongParams = {QueryParams.SIZE.key()};
        filterLongParams(parameters, document.getSet(), acceptedLongParams);

        // Check if the samples exist.
        if (parameters.containsKey(QueryParams.SAMPLE_IDS.key())) {
            if (document.getSet().containsKey(QueryParams.ID.key())) {
                throw new CatalogDBException("Updating file path/id and list of samples at the same time is forbidden.");
            }

            // Conver to set to remove possible duplicates
            Set<String> sampleIdList = new HashSet<>(parameters.getAsStringList(QueryParams.SAMPLE_IDS.key()));
            Query sampleQuery = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIdList);
            OpenCGAResult<Sample> sampleOpenCGAResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(clientSession, sampleQuery,
                    SampleManager.INCLUDE_SAMPLE_IDS);
            if (sampleOpenCGAResult.getNumResults() != sampleIdList.size()) {
                Set<String> foundSampleIds = sampleOpenCGAResult.getResults().stream()
                        .flatMap(s -> Stream.of(s.getId(), s.getUuid()))
                        .collect(Collectors.toSet());
                List<String> notFoundSamples = new ArrayList<>(sampleIdList.size());
                for (String sampleId : sampleIdList) {
                    if (!foundSampleIds.contains(sampleId)) {
                        notFoundSamples.add(sampleId);
                    }
                }

                throw new CatalogDBException("Samples '" + StringUtils.join(notFoundSamples, ",") + "' were not found.");
            }

            List<Sample> sampleList = sampleOpenCGAResult.getResults();

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            BasicUpdateAction operation = BasicUpdateAction.from(actionMap, QueryParams.SAMPLE_IDS.key(), BasicUpdateAction.ADD);

            OpenCGAResult<Document> fileResult = nativeGet(clientSession, query, new QueryOptions());
            // We obtain the list of fileIds to be added/removed for each file
            for (Document fileDocument : fileResult.getResults()) {
                getSampleChanges(fileDocument, sampleList, document, operation);
            }

            if (BasicUpdateAction.SET.equals(operation) || !sampleList.isEmpty()) {
                switch (operation) {
                    case SET:
                        document.getSet().put(PRIVATE_SAMPLES, fileConverter.convertSamples(sampleList));
                        break;
                    case REMOVE:
                        document.getPullAll().put(PRIVATE_SAMPLES, fileConverter.convertSamples(sampleList));
                        break;
                    case ADD:
                        document.getAddToSet().put(PRIVATE_SAMPLES, fileConverter.convertSamples(sampleList));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown operation " + operation);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] acceptedObjectParams = {QueryParams.INTERNAL_INDEX.key(), QueryParams.SOFTWARE.key(), QueryParams.EXPERIMENT.key(),
                QueryParams.STATUS.key(), QueryParams.INTERNAL_MISSING_SAMPLES.key(), QueryParams.QUALITY_CONTROL.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    private File checkOnlyOneFileMatches(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query tmpQuery = new Query(query);

        OpenCGAResult<File> fileResult = get(clientSession, tmpQuery, new QueryOptions());
        if (fileResult.getNumResults() == 0) {
            throw new CatalogDBException("Update file: No file found to be updated");
        }
        if (fileResult.getNumResults() > 1) {
            throw CatalogDBException.cannotUpdateMultipleEntries(QueryParams.ID.key(), "file");
        }
        return fileResult.first();
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

    @Override
    public int getFileSampleLinkThreshold() {
        return fileSampleLinkThreshold;
    }

    @Override
    public void setFileSampleLinkThreshold(int numSamples) {
        this.fileSampleLinkThreshold = numSamples;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document fileDocument, String status)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        long fileUid = fileDocument.getLong(PRIVATE_UID);
        long studyUid = fileDocument.getLong(PRIVATE_STUDY_UID);
        String fileId = fileDocument.getString(QueryParams.ID.key());
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
            // Delete file references from all referenced samples
            dbAdaptorFactory.getCatalogSampleDBAdaptor().removeFileReferences(clientSession, studyUid, fileId);

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
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<File> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    public OpenCGAResult<File> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<File> dbIterator = iterator(clientSession, query, options)) {
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
        return iterator(null, query, options);
    }

    public DBIterator<File> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
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
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
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
        Function<Document, Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
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
        qOptions = fixQueryOptions(qOptions);

        logger.debug("File query: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return fileCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedFileCollection.iterator(clientSession, bson, null, null, qOptions);
        }
    }

    private QueryOptions fixQueryOptions(QueryOptions qOptions) {
        QueryOptions options = removeAnnotationProjectionOptions(qOptions);
        options = filterOptions(options, FILTER_ROUTE_FILES);
        options = changeProjectionKey(options, QueryParams.SAMPLE_IDS.key(), PRIVATE_SAMPLES);
        fixAclProjection(options);
        return options;
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
    public <T> OpenCGAResult<T> distinct(long studyUid, String field, Query query, String userId, Class<T> clazz)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(fileCollection.distinct(field, bson, clazz));
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

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.FILE, user, configuration));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FileAclEntry.FilePermissions.VIEW_ANNOTATIONS.name(),
                            Enums.Resource.FILE, configuration));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FileAclEntry.FilePermissions.VIEW.name(),
                            Enums.Resource.FILE, configuration));
                }
            }

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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), myQuery, QueryParams.STATUS_ID.type(),
                                andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(FileStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), myQuery,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_INDEX_STATUS_NAME:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(FileIndex.IndexStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
                        addAutoOrQuery(queryParam.key(), queryParam.key(), myQuery, queryParam.type(), andBsonList);
                        break;
                    case SAMPLE_IDS:
                        List<Bson> queryList = new ArrayList<>();
                        addAutoOrQuery(PRIVATE_SAMPLES + "." + SampleDBAdaptor.QueryParams.ID.key(), queryParam.key(), myQuery,
                                QueryParam.Type.TEXT_ARRAY, queryList);
                        addAutoOrQuery(PRIVATE_SAMPLES + "." + SampleDBAdaptor.QueryParams.UUID.key(), queryParam.key(), myQuery,
                                QueryParam.Type.TEXT_ARRAY, queryList);
                        andBsonList.add(Filters.or(queryList));
                        break;
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
                    case EXTERNAL:
                    case TYPE:
                    case URI:
                    case ID:
                    case PATH:
                    case RELEASE:
                    case TAGS:
                    case SIZE:
                    case SOFTWARE_NAME:
                    case JOB_ID:
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
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(fileCollection, studyId, permissionRuleId);
    }

    void removeSampleReferences(ClientSession clientSession, long studyUid, Sample sample)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.SAMPLE_IDS.key(), sample.getId());

        ObjectMap params = new ObjectMap()
                .append(QueryParams.SAMPLE_IDS.key(), Collections.singletonList(sample.getId()));
        // Add the the Remove action for the sample provided
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS,
                new ObjectMap(QueryParams.SAMPLE_IDS.key(), BasicUpdateAction.REMOVE.name()));

        Document update = getValidatedUpdateParams(clientSession, studyUid, params, query, queryOptions).toFinalUpdateDocument();

        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);

        Bson bsonQuery = parseQuery(query);

        logger.debug("Sample references extraction. Query: {}, update: {}",
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult result = fileCollection.update(clientSession, bsonQuery, update, multi);
        logger.debug("Sample '" + sample.getId() + "' references removed from " + result.getNumUpdated() + " out of "
                + result.getNumMatches() + " files");
    }
}
