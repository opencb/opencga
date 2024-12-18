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

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.ParamUtils.BasicUpdateAction;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
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

    public static final String REVERSE_NAME = "_reverse";
    public static final String PRIVATE_SAMPLES = "_samples";
    private final MongoDBCollection fileCollection;
    private final MongoDBCollection deletedFileCollection;
    private FileConverter fileConverter;
    private int fileSampleLinkThreshold = 5000;

    private final IOManagerFactory ioManagerFactory;

    private enum UpdateAttributeParams {
        NESTED_PATH_UPDATE,
        PREVIOUS_FILE_ID,
        NEW_FILE_ID,
        SET_SAMPLES,
        ADDED_SAMPLES,
        REMOVED_SAMPLES
    }

    /***
     * CatalogMongoFileDBAdaptor constructor.
     *  @param fileCollection MongoDB connection to the file collection.
     * @param deletedFileCollection MongoDB connection to the file collection containing the deleted documents.
     * @param configuration Configuration file.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     * @param ioManagerFactory IOManagerFactory.
     */
    public FileMongoDBAdaptor(MongoDBCollection fileCollection, MongoDBCollection deletedFileCollection, Configuration configuration,
                              OrganizationMongoDBAdaptorFactory dbAdaptorFactory, IOManagerFactory ioManagerFactory) {
        super(configuration, LoggerFactory.getLogger(FileMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.deletedFileCollection = deletedFileCollection;
        this.fileConverter = new FileConverter();
        this.ioManagerFactory = ioManagerFactory;
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
                                List<VariableSet> variableSetList, QueryOptions options) throws CatalogException {
        return runTransaction(
                (clientSession) -> {
                    long tmpStartTime = startQuery();
                    logger.debug("Starting file insert transaction for file id '{}'", file.getId());

                    dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                    List<Event> events = insert(clientSession, studyId, file, existingSamples, nonExistingSamples, variableSetList);
                    return endWrite(tmpStartTime, 1, 1, 0, 0, events);
                },
                (e) -> logger.error("Could not create file {}: {}", file.getId(), e.getMessage()));
    }

    @Override
    public OpenCGAResult insertWithVirtualFile(long studyId, File file, File virtualFile, List<Sample> existingSamples,
                                               List<Sample> nonExistingSamples, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogException {
        return runTransaction(
                (clientSession) -> {
                    long tmpStartTime = startQuery();
                    logger.debug("Starting file insert transaction for file id '{}' and virtual file id '{}'", file.getId(),
                            virtualFile.getId());

                    dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
                    List<Event> events = new ArrayList<>();
                    List<Event> tmpEvents = insert(clientSession, studyId, file, null, null, variableSetList);
                    events.addAll(tmpEvents);

                    Map<String, Object> actionMap = new HashMap<>();
                    actionMap.put(QueryParams.RELATED_FILES.key(), BasicUpdateAction.ADD);
                    QueryOptions qOptions = new QueryOptions(Constants.ACTIONS, actionMap);
                    if (virtualFile.getUid() <= 0) {
                        // Add multipart file and insert virtual file
                        virtualFile.setRelatedFiles(Collections.singletonList(
                                new FileRelatedFile(file, FileRelatedFile.Relation.MULTIPART))
                        );
                        tmpEvents = insert(clientSession, studyId, virtualFile, existingSamples, nonExistingSamples, variableSetList);
                        events.addAll(tmpEvents);
                    } else {
                        // Add multipart file in virtual file
                        ObjectMap params = new ObjectMap(QueryParams.RELATED_FILES.key(), Collections.singletonList(
                                new FileRelatedFile(file, FileRelatedFile.Relation.MULTIPART)
                        ));
                        transactionalUpdate(clientSession, virtualFile, params, null, qOptions);
                    }

                    // Add multipart file in physical file
                    ObjectMap params = new ObjectMap(QueryParams.RELATED_FILES.key(), Collections.singletonList(
                            new FileRelatedFile(virtualFile, FileRelatedFile.Relation.MULTIPART)
                    ));
                    transactionalUpdate(clientSession, file, params, null, qOptions);

                    return endWrite(tmpStartTime, 1, 1, 0, 0, events);
                },
                (e) -> logger.error("Could not create file {}: {}", file.getId(), e.getMessage()));
    }

    List<Event> insert(ClientSession clientSession, long studyId, File file, List<Sample> existingSamples, List<Sample> nonExistingSamples,
                       List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (filePathExists(clientSession, studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File", studyId, "path", file.getPath());
        }
        if (existingSamples == null) {
            existingSamples = Collections.emptyList();
        }
        if (nonExistingSamples == null) {
            nonExistingSamples = Collections.emptyList();
        }
        List<Event> eventList = new LinkedList<>();

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
                for (List<Sample> sampleList : sampleListList) {
                    logger.debug("Updating list of fileIds in batch of {} samples...", sampleList.size());

                    for (Sample sample : sampleList) {
                        dbAdaptorFactory.getCatalogSampleDBAdaptor().transactionalUpdate(clientSession, sample, params, null,
                                sampleUpdateOptions);
                    }

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
        long fileUid = getNewUid(clientSession);
        file.setUid(fileUid);
        file.setStudyUid(studyId);
        if (StringUtils.isEmpty(file.getUuid())) {
            file.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
        }

        List<Event> events = adjustAlignmentRelatedFiles(clientSession, file);
        eventList.addAll(events);
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

        return eventList;
    }

    private List<Event> adjustAlignmentRelatedFiles(ClientSession clientSession, File file)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        if (!file.getBioformat().equals(File.Bioformat.ALIGNMENT)) {
            return Collections.emptyList();
        }

        List<String> path;
        switch (file.getFormat()) {
            case BIGWIG:
                path = Collections.singletonList(file.getPath().replace(".bw", ""));
                break;
            case BAM:
                path = Arrays.asList(file.getPath() + ".bai", file.getPath() + ".bw");
                break;
            case BAI:
                path = Collections.singletonList(file.getPath().replace(".bai", ""));
                break;
            case CRAM:
                path = Collections.singletonList(file.getPath() + ".crai");
                break;
            case CRAI:
                path = Collections.singletonList(file.getPath().replace(".crai", ""));
                break;
            default:
                return Collections.emptyList();
        }

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), file.getStudyUid())
                .append(QueryParams.PATH.key(), path);
        QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.UID.key(), FileDBAdaptor.QueryParams.FORMAT.key(), FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key(), QueryParams.RELATED_FILES.key(),
                FileDBAdaptor.QueryParams.STUDY_UID.key(), QueryParams.INTERNAL_ALIGNMENT.key()));
        OpenCGAResult<File> result = get(clientSession, query, fileOptions);
        List<Event> eventList = new ArrayList<>();
        if (result.getNumResults() > 0) {
            switch (file.getFormat()) {
                case BIGWIG:
                    associateBigWigToBamFile(clientSession, file, result.first(), eventList);
                    break;
                case BAM:
                    for (File tmpResult : result.getResults()) {
                        if (tmpResult.getFormat().equals(File.Format.BAI)) {
                            associateAlignmentFileToIndexFile(clientSession, file, tmpResult, true, eventList);
                        } else if (tmpResult.getFormat().equals(File.Format.BIGWIG)) {
                            associateBamFileToBigWigFile(clientSession, file, tmpResult, true, eventList);
                        }
                    }
                    break;
                case BAI:
                case CRAI:
                    associateIndexFileToAlignmentFile(clientSession, file, result.first(), eventList);
                    break;
                case CRAM:
                    associateAlignmentFileToIndexFile(clientSession, file, result.first(), true, eventList);
                    break;
                default:
                    break;
            }
        }
        return eventList;
    }

    @Override
    public void associateAlignmentFiles(long studyUid) throws CatalogException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.FORMAT.key(), Arrays.asList(File.Format.BAM, File.Format.CRAM));
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(QueryParams.UID.key(), QueryParams.PATH.key(),
                QueryParams.FORMAT.key(), QueryParams.RELATED_FILES.key(), QueryParams.INTERNAL.key(), QueryParams.STUDY_UID.key()));

        try (DBIterator<File> iterator = iterator(query, options)) {
            while (iterator.hasNext()) {
                File alignmentFile = iterator.next();
                List<String> additionalFiles = new ArrayList<>(2);
                if (alignmentFile.getInternal().getAlignment() == null || alignmentFile.getInternal().getAlignment().getIndex() == null
                        || StringUtils.isEmpty(alignmentFile.getInternal().getAlignment().getIndex().getFileId())) {
                    switch (alignmentFile.getFormat()) {
                        case BAM:
                            additionalFiles.add(alignmentFile.getPath() + ".bai");
                            break;
                        case CRAM:
                            additionalFiles.add(alignmentFile.getPath() + ".crai");
                            break;
                        default:
                            break;
                    }
                }
                if (alignmentFile.getFormat().equals(File.Format.BAM)
                        && (alignmentFile.getInternal().getAlignment() == null
                        || alignmentFile.getInternal().getAlignment().getCoverage() == null
                        || StringUtils.isEmpty(alignmentFile.getInternal().getAlignment().getCoverage().getFileId()))) {
                    additionalFiles.add(alignmentFile.getPath() + ".bw");
                }

                if (!additionalFiles.isEmpty()) {
                    runTransaction(session -> {
                        Query tmpQuery = new Query()
                                .append(QueryParams.STUDY_UID.key(), studyUid)
                                .append(QueryParams.PATH.key(), additionalFiles);
                        QueryOptions fileOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                                FileDBAdaptor.QueryParams.UID.key(), FileDBAdaptor.QueryParams.FORMAT.key(),
                                FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.URI.key(),
                                FileDBAdaptor.QueryParams.PATH.key(), QueryParams.RELATED_FILES.key(),
                                FileDBAdaptor.QueryParams.STUDY_UID.key(), QueryParams.INTERNAL_ALIGNMENT.key()));
                        try (DBIterator<File> tmpIterator = iterator(session, tmpQuery, fileOptions)) {
                            while (tmpIterator.hasNext()) {
                                File secondFile = tmpIterator.next();
                                switch (secondFile.getFormat()) {
                                    case BAI:
                                    case CRAI:
                                        associateAlignmentFileToIndexFile(session, alignmentFile, secondFile, new LinkedList<>());
                                        break;
                                    case BIGWIG:
                                        associateBamFileToBigWigFile(session, alignmentFile, secondFile, new LinkedList<>());
                                    default:
                                        break;
                                }
                            }
                        }
                        return null;
                    });
                }
            }
        }
    }

    private void associateBamFileToBigWigFile(ClientSession clientSession, File bamFile, File bigWigFile, List<Event> eventList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        associateBamFileToBigWigFile(clientSession, bamFile, bigWigFile, false, eventList);
    }

    private void associateBamFileToBigWigFile(ClientSession clientSession, File bamFile, File bigWigFile, boolean bamFileNotYetInserted,
                                              List<Event> eventList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        eventList.add(createAssociationInfoEvent(bamFile, bigWigFile));

        if (CollectionUtils.isNotEmpty(bigWigFile.getRelatedFiles())) {
            for (FileRelatedFile relatedFile : bigWigFile.getRelatedFiles()) {
                if (relatedFile.getRelation().equals(FileRelatedFile.Relation.ALIGNMENT)) {
                    eventList.add(createAssociationWarningEvent(bigWigFile, bamFile.getFormat(), bamFile.getPath()));
                }
            }
        }

        FileInternalCoverageIndex coverage = new FileInternalCoverageIndex(new InternalStatus(InternalStatus.READY), bigWigFile.getId(), "",
                -1);
        if (bamFileNotYetInserted) {
            // Add BIGWIG reference in BAM file
            bamFile.getInternal().getAlignment().setCoverage(coverage);
        } else {
            // Add coverage reference in BAM document
            addCoverageReferenceInBamFile(clientSession, bamFile, coverage);
        }

        // Add BAM file to list of related files in BIGWIG file
        addAlignmentReferenceToRelatedFiles(clientSession, bamFile, bigWigFile);
    }

    private void addCoverageReferenceInBamFile(ClientSession clientSession, File bamFile, FileInternalCoverageIndex coverage)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        ObjectMap params = new ObjectMap(QueryParams.INTERNAL_ALIGNMENT_COVERAGE.key(), coverage);
        transactionalUpdate(clientSession, bamFile, params, null, null);
    }

    private void addIndexReferenceInAlignmentFile(ClientSession clientSession, File alignFile, FileInternalAlignmentIndex index)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        ObjectMap params = new ObjectMap(QueryParams.INTERNAL_ALIGNMENT_INDEX.key(), index);
        transactionalUpdate(clientSession, alignFile, params, null, null);
    }

    private void addAlignmentReferenceToRelatedFiles(ClientSession clientSession, File bamFile, File targetFile)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        List<FileRelatedFile> tmpRelatedFileList = Collections.singletonList(new FileRelatedFile(bamFile,
                FileRelatedFile.Relation.ALIGNMENT));

        Map<String, Object> tmpActionMap = new HashMap<>();
        tmpActionMap.put(QueryParams.RELATED_FILES.key(), BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, tmpActionMap);

        ObjectMap params = new ObjectMap(QueryParams.RELATED_FILES.key(), tmpRelatedFileList);
        transactionalUpdate(clientSession, targetFile, params, null, options);
    }

    private void associateAlignmentFileToIndexFile(ClientSession clientSession, File alignFile, File indexFile, List<Event> eventList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        associateAlignmentFileToIndexFile(clientSession, alignFile, indexFile, false, eventList);
    }

    private void associateAlignmentFileToIndexFile(ClientSession clientSession, File alignFile, File indexFile,
                                                   boolean alignmentFileNotYetInserted, List<Event> eventList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        eventList.add(createAssociationInfoEvent(alignFile, indexFile));

        if (CollectionUtils.isNotEmpty(indexFile.getRelatedFiles())) {
            for (FileRelatedFile relatedFile : indexFile.getRelatedFiles()) {
                if (relatedFile.getRelation().equals(FileRelatedFile.Relation.ALIGNMENT)) {
                    eventList.add(createAssociationWarningEvent(indexFile, alignFile.getFormat(), alignFile.getPath()));
                }
            }
        }

        FileInternalAlignmentIndex index = new FileInternalAlignmentIndex(new InternalStatus(InternalStatus.READY), indexFile.getId(), "");
        if (alignmentFileNotYetInserted) {
            // Add index reference to alignment file
            alignFile.getInternal().getAlignment().setIndex(index);
        } else {
            addIndexReferenceInAlignmentFile(clientSession, alignFile, index);
        }

        // Add alignment file to list of related files in index file
        addAlignmentReferenceToRelatedFiles(clientSession, alignFile, indexFile);
    }

    private void associateBigWigToBamFile(ClientSession clientSession, File bigWig, File bamFile, List<Event> eventList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        eventList.add(createAssociationInfoEvent(bigWig, bamFile));

        if (bamFile.getInternal() != null && bamFile.getInternal().getAlignment() != null
                && bamFile.getInternal().getAlignment().getCoverage() != null
                && StringUtils.isNotEmpty(bamFile.getInternal().getAlignment().getCoverage().getFileId())) {
            eventList.add(createAssociationWarningEvent(bamFile, bigWig.getFormat(),
                    bamFile.getInternal().getAlignment().getCoverage().getFileId()));
        }

        List<FileRelatedFile> relatedFileList = bigWig.getRelatedFiles() != null ? new ArrayList<>(bigWig.getRelatedFiles())
                : new ArrayList<>();
        relatedFileList.add(new FileRelatedFile(bamFile, FileRelatedFile.Relation.ALIGNMENT));
        bigWig.setRelatedFiles(relatedFileList);

        FileInternalCoverageIndex coverage = new FileInternalCoverageIndex(new InternalStatus(InternalStatus.READY),
                bigWig.getId(), "", -1);
        bigWig.getInternal().getAlignment().setCoverage(coverage);

        // Add coverage reference to bam file
        addCoverageReferenceInBamFile(clientSession, bamFile, coverage);
    }

    private void associateIndexFileToAlignmentFile(ClientSession clientSession, File indexFile, File alignmentFile, List<Event> eventList)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        eventList.add(createAssociationInfoEvent(indexFile, alignmentFile));

        if (alignmentFile.getInternal() != null && alignmentFile.getInternal().getAlignment() != null
                && alignmentFile.getInternal().getAlignment().getIndex() != null
                && StringUtils.isNotEmpty(alignmentFile.getInternal().getAlignment().getIndex().getFileId())) {
            eventList.add(createAssociationWarningEvent(alignmentFile, indexFile.getFormat(),
                    alignmentFile.getInternal().getAlignment().getIndex().getFileId()));
        }

        List<FileRelatedFile> relatedFileList = indexFile.getRelatedFiles() != null ? new ArrayList<>(indexFile.getRelatedFiles())
                : new ArrayList<>();
        relatedFileList.add(new FileRelatedFile(alignmentFile, FileRelatedFile.Relation.ALIGNMENT));
        indexFile.setRelatedFiles(relatedFileList);

        FileInternalAlignmentIndex alignmentIndex = new FileInternalAlignmentIndex(new InternalStatus(InternalStatus.READY),
                indexFile.getId(), "");
        indexFile.getInternal().getAlignment().setIndex(alignmentIndex);

        addIndexReferenceInAlignmentFile(clientSession, alignmentFile, alignmentIndex);
    }

    private Event createAssociationInfoEvent(File firstFile, File secondFile) {
        logger.info("The {} file '{}' was automatically associated to the {} file '{}'.", firstFile.getFormat(), firstFile.getPath(),
                secondFile.getFormat(), secondFile.getPath());
        return new Event(Event.Type.INFO, "The " + firstFile.getFormat() + " file '" + firstFile.getPath() + "' was automatically"
                + " associated to the " + secondFile.getFormat() + " file '" + secondFile.getPath() + "'.");
    }

    private Event createAssociationWarningEvent(File file, File.Format format, String fileIdFound) {
        logger.warn("Found another {} file '{}' associated to the {} file '{}'. This relation was automatically removed.", format,
                fileIdFound, file.getFormat(), file.getPath());
        return new Event(Event.Type.WARNING, "Found another " + format + " file '" + fileIdFound + "' associated to the " + file.getFormat()
                + " file '" + file.getPath() + "'. This relation was automatically removed.");
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
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.PATH.key(), QueryParams.SIZE.key(),
                        QueryParams.STUDY_UID.key()));
        OpenCGAResult<File> fileDataResult = get(fileUid, options);

        if (fileDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update file. File uid '" + fileUid + "' not found.");
        }

        try {
            return runTransaction(clientSession -> transactionalUpdate(clientSession, fileDataResult.first(), parameters,
                    variableSetList, queryOptions));
        } catch (CatalogException e) {
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

        OpenCGAResult<File> result = OpenCGAResult.empty(File.class);

        while (iterator.hasNext()) {
            File file = iterator.next();
            try {
                result.append(runTransaction(clientSession -> transactionalUpdate(clientSession, file, parameters, variableSetList,
                        queryOptions)));
            } catch (CatalogException e) {
                logger.error("Could not update file {}: {}", file.getPath(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, file.getPath(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    @Override
    OpenCGAResult<File> transactionalUpdate(ClientSession clientSession, File file, ObjectMap parameters,
                                            List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        variableSetList = ParamUtils.defaultObject(variableSetList, Collections::emptyList);
        queryOptions = ParamUtils.defaultObject(queryOptions, QueryOptions::empty);

        long tmpStartTime = startQuery();
        long studyUid = file.getStudyUid();
        long fileUid = file.getUid();

        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.UID.key(), fileUid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.PATH.key(), QueryParams.URI.key(),
                        QueryParams.TYPE.key(), QueryParams.SIZE.key(), QueryParams.STUDY_UID.key()));
        File completeFile = get(clientSession, tmpQuery, options).first();

        // We perform the update.
        Bson queryBson = parseQuery(tmpQuery);
        DataResult result = updateAnnotationSets(clientSession, studyUid, fileUid, parameters, variableSetList,
                queryOptions, false);

        UpdateDocument updateDocument = getValidatedUpdateParams(clientSession, studyUid, parameters, tmpQuery, queryOptions);
        Document fileUpdate = updateDocument.toFinalUpdateDocument();

        if (fileUpdate.isEmpty() && result.getNumUpdated() == 0) {
            if (!parameters.isEmpty()) {
                logger.error("Non-processed update parameters: {}", parameters.keySet());
            }
            throw new CatalogDBException("Nothing to be updated");
        }

        List<Event> events = new ArrayList<>();
        if (!fileUpdate.isEmpty()) {
            logger.debug("Update file. Query: {}, Update: {}", queryBson.toBsonDocument(), fileUpdate.toBsonDocument());

            result = fileCollection.update(clientSession, queryBson, fileUpdate, null);

            // If the size of some of the files have been changed, notify to the correspondent study
            if (parameters.containsKey(QueryParams.SIZE.key())) {
                long newDiskUsage = parameters.getLong(QueryParams.SIZE.key());
                long difDiskUsage = newDiskUsage - completeFile.getSize();
                dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(clientSession, studyUid, difDiskUsage);
            }

            updateSampleReferences(clientSession, completeFile, updateDocument);
            updateWhenFileIdChanged(clientSession, completeFile, updateDocument, queryOptions);

            if (result.getNumMatches() == 0) {
                throw new CatalogDBException("File " + completeFile.getPath() + " not found");
            }
            if (result.getNumUpdated() == 0) {
                events.add(new Event(Event.Type.WARNING, completeFile.getPath(), "File was already updated"));
            }
            logger.debug("File {} successfully updated", completeFile.getPath());
        }

        return endWrite(tmpStartTime, 1, 1, events);
    }

    @Override
    OpenCGAResult<File> transactionalUpdate(ClientSession clientSession, long studyUid, Bson query, UpdateDocument updateDocument)
            throws CatalogDBException {
        long tmpStartTime = startQuery();

        Document fileUpdate = updateDocument.toFinalUpdateDocument();

        if (fileUpdate.isEmpty()) {
            throw new CatalogDBException("Nothing to be updated");
        }

        List<Event> events = new ArrayList<>();
        logger.debug("Update file. Query: {}, Update: {}", query.toBsonDocument(), fileUpdate.toBsonDocument());

        DataResult<?> result = fileCollection.update(clientSession, query, fileUpdate, new QueryOptions(MongoDBCollection.MULTI, true));
        logger.debug("{} file(s) successfully updated", result.getNumUpdated());

        return endWrite(tmpStartTime, result.getNumMatches(), result.getNumUpdated(), events);
    }

    private void updateSampleReferences(ClientSession clientSession, File file, UpdateDocument updateDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (!updateDocument.getAttributes().isEmpty()) {
            ObjectMap addedSamples = (ObjectMap) updateDocument.getAttributes().getMap(UpdateAttributeParams.ADDED_SAMPLES.name());
            ObjectMap removedSamples = (ObjectMap) updateDocument.getAttributes().getMap(UpdateAttributeParams.REMOVED_SAMPLES.name());
            List<Long> setSamples = updateDocument.getAttributes().getAsLongList(UpdateAttributeParams.SET_SAMPLES.name());

            if (CollectionUtils.isEmpty(setSamples) && MapUtils.isEmpty(addedSamples) && MapUtils.isEmpty(removedSamples)) {
                return;
            }

            Bson sampleBsonQuery;
            UpdateDocument sampleUpdate;
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

                dbAdaptorFactory.getCatalogSampleDBAdaptor().transactionalUpdate(clientSession, file.getStudyUid(), sampleBsonQuery,
                        sampleUpdate);
            }
            if (addedSamples != null && !addedSamples.isEmpty()) {
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), file.getStudyUid())
                        .append(SampleDBAdaptor.QueryParams.UID.key(), addedSamples.getAsLongList(file.getId()));
                sampleBsonQuery = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null);

                ObjectMap actionMap = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), BasicUpdateAction.ADD.name());
                QueryOptions sampleUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);
                updateDocument = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseAndValidateUpdateParams(clientSession,
                        file.getStudyUid(), params, query, sampleUpdateOptions);

                dbAdaptorFactory.getCatalogSampleDBAdaptor().transactionalUpdate(clientSession, file.getStudyUid(), sampleBsonQuery,
                        updateDocument);
            }
            if (removedSamples != null && !removedSamples.isEmpty()) {
                Query query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), file.getStudyUid())
                        .append(SampleDBAdaptor.QueryParams.UID.key(), removedSamples.getAsLongList(file.getId()));
                sampleBsonQuery = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null);

                ObjectMap actionMap = new ObjectMap(SampleDBAdaptor.QueryParams.FILE_IDS.key(), BasicUpdateAction.REMOVE.name());
                QueryOptions sampleUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);
                updateDocument = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseAndValidateUpdateParams(clientSession,
                        file.getStudyUid(), params, query, sampleUpdateOptions);

                dbAdaptorFactory.getCatalogSampleDBAdaptor().transactionalUpdate(clientSession, file.getStudyUid(), sampleBsonQuery,
                        updateDocument);
            }
        }
    }

    private void updateWhenFileIdChanged(ClientSession clientSession, File file, UpdateDocument updateDocument, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String newFileId = updateDocument.getSet().getString(QueryParams.ID.key());
        if (StringUtils.isEmpty(newFileId)) {
            return;
        }

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), file.getStudyUid())
                .append(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());
        Bson sampleBsonQuery = dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, null);

        // Replace the id for the new one
        UpdateDocument sampleUpdate = new UpdateDocument();
        sampleUpdate.getSet().append(SampleDBAdaptor.QueryParams.FILE_IDS.key() + ".$", newFileId);

        dbAdaptorFactory.getCatalogSampleDBAdaptor().transactionalUpdate(clientSession, file.getStudyUid(), sampleBsonQuery, sampleUpdate);

        String skipMoveFileString = "_SKIP_MOVE_FILE";
        if (file.getType().equals(File.Type.DIRECTORY)) {
            // We get the files and folders directly under this directory
            Query tmpQuery = new Query()
                    .append(QueryParams.STUDY_UID.key(), file.getStudyUid())
                    .append(QueryParams.DIRECTORY.key(), file.getPath());
            String newPath = updateDocument.getSet().getString(QueryParams.PATH.key());
            try (DBIterator<File> iterator = iterator(clientSession, tmpQuery, FileManager.INCLUDE_FILE_URI_PATH)) {
                while (iterator.hasNext()) {
                    File tmpFile = iterator.next();
                    String targetPath = newPath + tmpFile.getName();
                    if (File.Type.DIRECTORY.equals(tmpFile.getType())) {
                        targetPath = targetPath + "/";
                    }
                    ObjectMap updateMap = new ObjectMap(QueryParams.PATH.key(), targetPath);
                    QueryOptions queryOptions = new QueryOptions(skipMoveFileString, true);
                    OpenCGAResult<File> result = transactionalUpdate(clientSession, tmpFile, updateMap, null, queryOptions);
                    if (result.getNumUpdated() == 0) {
                        throw new CatalogDBException("Could not update path from '" + tmpFile.getPath() + "' to '" + targetPath + "'");
                    }
                }
            }
        }

        String newUri = updateDocument.getSet().getString(QueryParams.URI.key());
        if (file.getUri() != null && StringUtils.isNotEmpty(newUri) && !newUri.equals(file.getUri().toString())
                && !options.getBoolean(skipMoveFileString)) {
            // Move just the main folder/file
            logger.info("Move file from uri '{}' to '{}'", file.getUri(), newUri);
            try {
                ioManagerFactory.get(file.getUri()).move(file.getUri(), UriUtils.createUri(newUri));
            } catch (CatalogIOException | IOException | URISyntaxException e) {
                throw new CatalogDBException("Could not move file/folder physically", e);
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
            updateDocument.getAttributes().put(UpdateAttributeParams.SET_SAMPLES.name(), currentSampleUidList);
        } else if (BasicUpdateAction.SET.equals(operation) || BasicUpdateAction.ADD.equals(operation)) {
            // We will see which of the samples are actually new
            List<Long> samplesToAdd = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (!currentSampleUidList.contains(sample.getUid())) {
                    samplesToAdd.add(sample.getUid());
                }
            }

            if (!samplesToAdd.isEmpty()) {
                updateDocument.getAttributes().put(UpdateAttributeParams.ADDED_SAMPLES.name(), new ObjectMap(fileId, samplesToAdd));
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
                    updateDocument.getAttributes().put(UpdateAttributeParams.REMOVED_SAMPLES.name(),
                            new ObjectMap(fileId, samplesToRemove));
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
                updateDocument.getAttributes().put(UpdateAttributeParams.REMOVED_SAMPLES.name(), new ObjectMap(fileId, samplesToRemove));
            }
        }
    }

    private UpdateDocument getValidatedUpdateParams(ClientSession clientSession, long studyUid, ObjectMap parameters, Query query,
                                                    QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CHECKSUM.key(), QueryParams.JOB_ID.key(),
        };
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
            OpenCGAResult<File> results = get(clientSession, query, FileManager.INCLUDE_FILE_URI_PATH);
            if (results.getNumResults() > 1) {
                throw new CatalogDBException("Cannot set same path to multiple files.");
            }
            File currentFile = results.first();

            // Check desired path is not in use by other file
            String desiredPath = parameters.getString(QueryParams.PATH.key());
            desiredPath = FileUtils.fixPath(desiredPath, currentFile.getType());
            document.getSet().put(QueryParams.PATH.key(), desiredPath);

            if (desiredPath.equals(currentFile.getPath())) {
                throw new CatalogDBException("Nothing to do. The path of the file was already '" + desiredPath + "'.");
            }
            if (StringUtils.isEmpty(desiredPath)) {
                throw new CatalogDBException("Cannot rename root folder.");
            }
            Query tmpQuery = new Query()
                    .append(QueryParams.STUDY_UID.key(), studyUid)
                    .append(QueryParams.PATH.key(), desiredPath);
            OpenCGAResult<Long> countResult = count(clientSession, tmpQuery);
            if (countResult.getNumMatches() > 0) {
                throw new CatalogDBException("There already exists another file under path '" + desiredPath + "'.");
            }

            // Look for parent folder to check a few things more
            List<String> pathList = FileUtils.calculateAllPossiblePaths(desiredPath);
            tmpQuery = new Query()
                    .append(QueryParams.STUDY_UID.key(), studyUid)
                    .append(QueryParams.PATH.key(), pathList);
            OpenCGAResult<File> parentFiles = get(clientSession, tmpQuery, FileManager.INCLUDE_FILE_URI_PATH);
            if (parentFiles.getNumResults() + 1 < pathList.size()) {
                Set<String> presentFolders = parentFiles.getResults().stream().map(File::getPath).collect(Collectors.toSet());
                String missingFolders = pathList.stream().filter(p -> !presentFolders.contains(p)).collect(Collectors.joining(", "));
                throw new CatalogDBException("Can't move file to path '" + desiredPath + "'. Please, create missing parent folders: "
                        + missingFolders);
            }

            File desiredParentFolder = parentFiles.first();
            for (File tmpParentFile : parentFiles.getResults()) {
                // Look for the closest parentFolder
                if (tmpParentFile.getPath().length() > desiredParentFolder.getPath().length()) {
                    desiredParentFolder = tmpParentFile;
                }
            }

            boolean changeUri = true;
            if (desiredParentFolder.isExternal() && !currentFile.isExternal()) {
                throw new CatalogDBException("Cannot move file to path '" + desiredPath + "'. The folder '" + desiredParentFolder.getPath()
                        + "' is linked to the external physical uri '" + desiredParentFolder.getUri() + "' so OpenCGA can't move"
                        + " anything there.");
            } else if (desiredParentFolder.isExternal() && currentFile.isExternal()) {
                changeUri = false;
            }

            if (changeUri) {
                URI fileUri = null;
                try {
                    fileUri = FileUtils.getFileUri(desiredPath, desiredParentFolder, currentFile.getType());
                } catch (URISyntaxException e) {
                    throw new CatalogDBException("Could not update file.", e);
                }
                document.getSet().put(QueryParams.URI.key(), fileUri.toString());
            }

            String name = FileUtils.getFileName(desiredPath);
            String newFileId = FileUtils.getFileId(desiredPath);
            document.getSet().put(QueryParams.ID.key(), newFileId);
            document.getSet().put(QueryParams.NAME.key(), name);
            document.getSet().put(REVERSE_NAME, StringUtils.reverse(name));

            document.getAttributes().put(UpdateAttributeParams.PREVIOUS_FILE_ID.name(), currentFile.getId());
            document.getAttributes().put(UpdateAttributeParams.NEW_FILE_ID.name(), newFileId);
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
                    List<Document> documentList = fixRelatedFilesForRemoval(relatedFileDocument);
                    document.getPull().put(QueryParams.RELATED_FILES.key(), documentList);
                    break;
                case ADD:
                    document.getAddToSet().put(QueryParams.RELATED_FILES.key(), relatedFileDocument);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation " + operation);
            }
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

        String[] acceptedObjectParams = {QueryParams.INTERNAL_VARIANT_INDEX.key(), QueryParams.INTERNAL_VARIANT_ANNOTATION_INDEX.key(),
                QueryParams.INTERNAL_VARIANT_SECONDARY_INDEX.key(), QueryParams.INTERNAL_VARIANT_SECONDARY_ANNOTATION_INDEX.key(),
                QueryParams.INTERNAL_ALIGNMENT_INDEX.key(), QueryParams.INTERNAL_ALIGNMENT_COVERAGE.key(), QueryParams.SOFTWARE.key(),
                QueryParams.EXPERIMENT.key(), QueryParams.STATUS.key(), QueryParams.INTERNAL_MISSING_SAMPLES.key(),
                QueryParams.QUALITY_CONTROL.key(), QueryParams.INTERNAL_STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_ID.key())) {
            FileStatus fileStatus = new FileStatus(parameters.getString(QueryParams.INTERNAL_STATUS_ID.key()));
            Document fileStatusDoc = getMongoDBDocument(fileStatus, QueryParams.INTERNAL_STATUS.key());
            document.getSet().put(QueryParams.INTERNAL_STATUS.key(), fileStatusDoc);
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

    private List<Document> fixRelatedFilesForRemoval(List<Document> relatedFiles) {
        if (CollectionUtils.isEmpty(relatedFiles)) {
            return Collections.emptyList();
        }

        List<Document> relatedFilesCopy = new ArrayList<>(relatedFiles.size());
        for (Document relatedFile : relatedFiles) {
            relatedFilesCopy.add(new Document("file", new Document("uid", relatedFile.get("file", Document.class).get("uid"))));
        }
        return relatedFilesCopy;
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
        } catch (CatalogException e) {
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
            } catch (CatalogException e) {
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

                removeFileReferences(clientSession, studyUid, tmpFileUid, tmpFile);
                dbAdaptorFactory.getCatalogJobDBAdaptor().removeFileReferences(clientSession, studyUid, tmpFileUid, tmpFile);
                dbAdaptorFactory.getClinicalAnalysisDBAdaptor().removeFileReferences(clientSession, studyUid, tmpFileUid, tmpFile);

                // Set status
                nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new FileStatus(status), "status"), tmpFile);

                // Insert the document in the DELETE collection
                deletedFileCollection.insert(clientSession, replaceDotsInKeys(tmpFile), null);
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

    void removeFileReferences(ClientSession clientSession, long studyUid, long fileUid, Document fileDoc)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        File file = fileConverter.convertToDataModelType(fileDoc);

        // Remove file references from relatedFiles array
        FileRelatedFile relatedFile = new FileRelatedFile(file, null);
        ObjectMap parameters = new ObjectMap(QueryParams.RELATED_FILES.key(), Collections.singletonList(relatedFile));
        ObjectMap actionMap = new ObjectMap(QueryParams.RELATED_FILES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.RELATED_FILES_FILE_UID.key(), fileUid);
        UpdateDocument updateDocument = getValidatedUpdateParams(clientSession, studyUid, parameters, query, options);
        Document updateDoc = updateDocument.toFinalUpdateDocument();
        if (!updateDoc.isEmpty()) {
            Bson bsonQuery = parseQuery(query);
            OpenCGAResult<File> result = transactionalUpdate(clientSession, studyUid, bsonQuery, updateDocument);
            if (result.getNumUpdated() > 0) {
                logger.debug("File '{}' removed from related files array from {} files.", file.getPath(), result.getNumUpdated());
            }
        }

        // Remove file references from alignment indexes
        FileInternalAlignmentIndex alignmentIndex = new FileInternalAlignmentIndex(new InternalStatus(InternalStatus.DELETED), "", "");
        parameters = new ObjectMap(QueryParams.INTERNAL_ALIGNMENT_INDEX.key(), alignmentIndex);
        query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.INTERNAL_ALIGNMENT_INDEX_FILE_ID.key(), file.getId());
        updateDocument = getValidatedUpdateParams(clientSession, studyUid, parameters, query, options);
        updateDoc = updateDocument.toFinalUpdateDocument();
        if (!updateDoc.isEmpty()) {
            Bson bsonQuery = parseQuery(query);
            OpenCGAResult<File> result = transactionalUpdate(clientSession, studyUid, bsonQuery, updateDocument);
            if (result.getNumUpdated() > 0) {
                logger.debug("File '{}' removed from internal.alignment.index object", file.getPath());
            }
        }

        FileInternalCoverageIndex coverageIndex = new FileInternalCoverageIndex(new InternalStatus(InternalStatus.DELETED), "", "", 0);
        parameters = new ObjectMap(QueryParams.INTERNAL_ALIGNMENT_COVERAGE.key(), coverageIndex);
        query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.INTERNAL_ALIGNMENT_COVERAGE_FILE_ID.key(), file.getId());
        updateDocument = getValidatedUpdateParams(clientSession, studyUid, parameters, query, options);
        updateDoc = updateDocument.toFinalUpdateDocument();
        if (!updateDoc.isEmpty()) {
            Bson bsonQuery = parseQuery(query);
            OpenCGAResult<File> result = transactionalUpdate(clientSession, studyUid, bsonQuery, updateDocument);
            if (result.getNumUpdated() > 0) {
                logger.debug("File '{}' removed from internal.alignment.coverage object", file.getPath());
            }
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
        logger.debug("File count: query : {}", bson.toBsonDocument());
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

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult<Document> nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
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
                options);
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
                queryOptions);
    }

    @Override
    public DBIterator<File> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(null, query, options, user);

        Document studyDocument = getStudyDocument(null, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(dbAdaptorFactory.getOrganizationId(), studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_FILE_ANNOTATIONS.name(),
                FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileCatalogMongoDBIterator<File>(mongoCursor, null, fileConverter, iteratorFilter, this,
                studyUid, user, options);
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
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(dbAdaptorFactory.getOrganizationId(), studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_FILE_ANNOTATIONS.name(),
                FilePermissions.VIEW_ANNOTATIONS.name());

        return new FileCatalogMongoDBIterator<>(mongoCursor, null, null, iteratorFilter, this,
                studyUid, user, options);
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

        logger.debug("File query: {}", bson.toBsonDocument());
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

        // type must always be there when relatedFiles is included
        options = filterQueryOptionsToIncludeKeys(options, Collections.singletonList(QueryParams.TYPE.key()));

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
    public OpenCGAResult distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(fileCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        StopWatch stopWatch = StopWatch.createStarted();
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        Set<String> results = new LinkedHashSet<>();
        for (String field : fields) {
            results.addAll(fileCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
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
            boolean simplifyPermissions = simplifyPermissions();

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.FILE, user,
                        simplifyPermissions));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FilePermissions.VIEW_ANNOTATIONS.name(),
                            Enums.Resource.FILE, simplifyPermissions));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, FilePermissions.VIEW.name(),
                            Enums.Resource.FILE, simplifyPermissions));
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
                    case INTERNAL_VARIANT_ANNOTATION_INDEX_STATUS_ID:
                    case INTERNAL_VARIANT_SECONDARY_INDEX_STATUS_ID:
                    case INTERNAL_VARIANT_SECONDARY_ANNOTATION_INDEX_STATUS_ID:
                    case INTERNAL_ALIGNMENT_INDEX_STATUS_ID:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(FileStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), myQuery,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_VARIANT_INDEX_STATUS_ID:
                        // Convert the status to a positive status
                        myQuery.put(queryParam.key(),
                                InternalStatus.getPositiveStatus(VariantIndexStatus.STATUS_LIST, myQuery.getString(queryParam.key())));
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
                    case UUID:
                    case EXTERNAL:
                    case TYPE:
                    case URI:
                    case ID:
                    case PATH:
                    case RELEASE:
                    case FORMAT:
                    case BIOFORMAT:
                    case TAGS:
                    case SIZE:
                    case SOFTWARE_NAME:
                    case RELATED_FILES_FILE_UID:
                    case INTERNAL_ALIGNMENT_INDEX_FILE_ID:
                    case INTERNAL_ALIGNMENT_COVERAGE_FILE_ID:
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

        logger.debug("Sample references extraction. Query: {}, update: {}", bsonQuery.toBsonDocument(), update.toBsonDocument());
        DataResult result = fileCollection.update(clientSession, bsonQuery, update, multi);
        logger.debug("Sample '" + sample.getId() + "' references removed from " + result.getNumUpdated() + " out of "
                + result.getNumMatches() + " files");
    }
}
