
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

package org.opencb.opencga.analysis.variant.metadata;

import com.mongodb.MongoServerException;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.IndexStatus;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ID;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.URI;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStorageMetadataSynchronizer {

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.ID.key(),
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.PATH.key(),
            FileDBAdaptor.QueryParams.URI.key(),
            FileDBAdaptor.QueryParams.SAMPLE_IDS.key(),
            FileDBAdaptor.QueryParams.INTERNAL.key(),
            FileDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final Query INDEXED_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), VariantIndexStatus.READY)
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    public static final Query RUNNING_INDEX_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), Arrays.asList(VariantIndexStatus.LOADING, VariantIndexStatus.INDEXING))
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    public static final QueryOptions COHORT_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            CohortDBAdaptor.QueryParams.ID.key(),
            CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
            CohortDBAdaptor.QueryParams.SAMPLES.key() + "." + SampleDBAdaptor.QueryParams.ID.key()
    ));
    public static final QueryOptions SAMPLE_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            SampleDBAdaptor.QueryParams.ID.key(),
            SampleDBAdaptor.QueryParams.INTERNAL_VARIANT.key()
    ));

    protected static Logger logger = LoggerFactory.getLogger(CatalogStorageMetadataSynchronizer.class);

    private final CatalogManager catalogManager;
    private final VariantStorageMetadataManager metadataManager;


    public CatalogStorageMetadataSynchronizer(CatalogManager catalogManager, VariantStorageMetadataManager metadataManager) {
        this.catalogManager = catalogManager;
        this.metadataManager = metadataManager;
    }

    public static void updateProjectMetadata(CatalogManager catalog, VariantStorageMetadataManager scm, String project, String sessionId)
            throws CatalogException, StorageEngineException {
        final Project p = catalog.getProjectManager().get(project,
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                ProjectDBAdaptor.QueryParams.ORGANISM.key(), ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key())),
                        sessionId)
                .first();

        updateProjectMetadata(scm, p.getOrganism(), p.getCurrentRelease());
    }

    public static void updateProjectMetadata(VariantStorageMetadataManager scm, ProjectOrganism organism, int release)
            throws StorageEngineException {
        String scientificName = AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName(organism.getScientificName());

        scm.updateProjectMetadata(projectMetadata -> {
            if (projectMetadata == null) {
                projectMetadata = new ProjectMetadata();
            }
            projectMetadata.setSpecies(scientificName);
            projectMetadata.setAssembly(organism.getAssembly());
            projectMetadata.setRelease(release);
            return projectMetadata;
        });
    }

    public StudyMetadata getStudyMetadata(String study) throws CatalogException {
        return metadataManager.getStudyMetadata(study);
    }

    /**
     * Updates catalog metadata from storage metadata.
     *
     * @param study     StudyMetadata
     * @param files     Files to update
     * @param sessionId User session id
     * @return if there were modifications in catalog
     * @throws CatalogException if there is an error with catalog
     */
    public boolean synchronizeCatalogFilesFromStorage(String study, List<File> files, String sessionId, QueryOptions fileQueryOptions)
            throws CatalogException, StorageEngineException {

        boolean modified = synchronizeCatalogFilesFromStorage(study, files, sessionId);
        if (modified) {
            // Files updated. Reload files from catalog
            for (int i = 0; i < files.size(); i++) {
                File file = files.get(i);
                files.set(i, catalogManager.getFileManager().get(study, file.getId(), fileQueryOptions, sessionId).first());
            }
        }
        return modified;
    }

    /**
     * Updates catalog metadata from storage metadata.
     *
     * @param studyFqn  Study FQN
     * @param files     Files to update
     * @param sessionId User session id
     * @return if there were modifications in catalog
     * @throws CatalogException if there is an error with catalog
     */
    public boolean synchronizeCatalogFilesFromStorage(String studyFqn, List<File> files, String sessionId)
            throws CatalogException {
        StudyMetadata study = metadataManager.getStudyMetadata(studyFqn);
        if (study == null) {
            return false;
        }
        // Update Catalog file and cohort status.
        logger.info("Synchronizing study " + study.getName());

        if (files != null && files.isEmpty()) {
            files = null;
        }

        return synchronizeFiles(study, files, sessionId);
    }

    /**
     * Updates catalog metadata from storage metadata.
     *
     * @param studyFqn  Study FQN
     * @param samples   Samples to update
     * @param sessionId User session id
     * @return if there were modifications in catalog
     * @throws CatalogException if there is an error with catalog
     */
    public boolean synchronizeCatalogSamplesFromStorage(String studyFqn, List<String> samples, String sessionId)
            throws CatalogException {
        StudyMetadata study = metadataManager.getStudyMetadata(studyFqn);
        if (study == null) {
            return false;
        }
        logger.info("Synchronizing samples from study " + study.getName());

        List<Integer> sampleIds;
        if (CollectionUtils.isEmpty(samples) || samples.size() == 1 && samples.get(0).equals(ParamConstants.ALL)) {
            sampleIds = new LinkedList<>();
            for (SampleMetadata sampleMetadata : metadataManager.sampleMetadataIterable(study.getId())) {
                sampleIds.add(sampleMetadata.getId());
            }
        } else {
            sampleIds = new ArrayList<>(samples.size());
            for (String sample : samples) {
                sampleIds.add(metadataManager.getSampleId(study.getId(), sample));
            }
        }

        return synchronizeSamples(study, sampleIds, sessionId);
    }


    /**
     * Synchronize all studies from storage.
     *
     * @param token user token
     * @return if anything was modified
     * @throws CatalogException on error
     */
    public boolean synchronizeCatalogFromStorage(String token) throws CatalogException {
        boolean modified = false;
        for (String study : metadataManager.getStudyNames()) {
            modified |= synchronizeCatalogStudyFromStorage(study, token);
        }
        return modified;
    }

    public boolean synchronizeCatalogStudyFromStorage(String study, String sessionId)
            throws CatalogException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        if (studyMetadata != null) {
            // Update Catalog file and cohort status.
            return synchronizeCatalogStudyFromStorage(studyMetadata, sessionId);
        } else {
            return false;
        }
    }

    /**
     * Updates catalog metadata from storage metadata.
     * <p>
     * 1) Update cohort ALL
     * 2) Update cohort status (calculating / invalid)
     * 3) Update file status
     *
     * @param study     StudyMetadata
     * @param sessionId User session id
     * @return if there were modifications in catalog
     * @throws CatalogException if there is an error with catalog
     */
    public boolean synchronizeCatalogStudyFromStorage(StudyMetadata study, String sessionId)
            throws CatalogException {
        logger.info("Synchronizing study " + study.getName());

        boolean modified = synchronizeFiles(study, null, sessionId);

        modified |= synchronizeCohorts(study, sessionId);

        return modified;
    }

    public boolean synchronizeCohorts(String study, String sessionId) throws CatalogException {
        StudyMetadata studyMetadata = getStudyMetadata(study);
        if (studyMetadata == null) {
            return false;
        } else {
            return synchronizeCohorts(studyMetadata, sessionId);
        }
    }

    protected boolean synchronizeCohorts(StudyMetadata study, String sessionId) throws CatalogException {
        boolean modified = false;

        // -------------------------------------------------------------------
        logger.info("Synchronize catalog cohorts from Storage");
        // -------------------------------------------------------------------
        //Check if cohort ALL has been modified
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        CohortMetadata defaultCohortStorage = metadataManager.getCohortMetadata(study.getId(), defaultCohortName);
        if (defaultCohortStorage != null) {
            Set<String> cohortFromStorage = defaultCohortStorage.getSamples()
                    .stream()
                    .map(id -> metadataManager.getSampleName(study.getId(), id))
                    .collect(Collectors.toSet());
            Cohort defaultCohort = catalogManager.getCohortManager()
                    .get(study.getName(), defaultCohortName, COHORT_QUERY_OPTIONS, sessionId).first();
            List<String> cohortFromCatalog = defaultCohort
                    .getSamples()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (cohortFromCatalog.size() != cohortFromStorage.size() || !cohortFromStorage.containsAll(cohortFromCatalog)) {
                if (defaultCohort.getInternal().getStatus().getId().equals(CohortStatus.CALCULATING)) {
                    String status;
                    if (defaultCohortStorage.isInvalid()) {
                        status = CohortStatus.INVALID;
                    } else if (defaultCohortStorage.isStatsReady()) {
                        status = CohortStatus.READY;
                    } else {
                        status = CohortStatus.NONE;
                    }
                    catalogManager.getCohortManager().setStatus(study.getName(), defaultCohortName, status, null, sessionId);
                }
                logger.info("Update cohort " + defaultCohortName);
                QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                        ParamUtils.BasicUpdateAction.SET));
                List<SampleReferenceParam> samples = cohortFromStorage.stream().map(s -> new SampleReferenceParam().setId(s))
                        .collect(Collectors.toList());
                catalogManager.getCohortManager().update(study.getName(), defaultCohortName,
                        new CohortUpdateParams().setSamples(samples),
                        true, options, sessionId);
                modified = true;
            }
        } else {
            logger.info("Cohort " + defaultCohortName + " not found in variant storage");
        }

        Map<String, CohortMetadata> calculatedStats = new HashMap<>();
        for (CohortMetadata cohortMetadata : metadataManager.getCalculatedCohorts(study.getId())) {
            calculatedStats.put(cohortMetadata.getName(), cohortMetadata);
        }
        //Check if any cohort stat has been updated
        if (!calculatedStats.isEmpty()) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), calculatedStats.keySet());

            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(study.getName(),
                    query, COHORT_QUERY_OPTIONS, sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    CohortMetadata storageCohort = calculatedStats.get(cohort.getId());
                    if (cohort.getInternal().getStatus() != null && cohort.getInternal().getStatus().getId().equals(CohortStatus.INVALID)) {
                        if (cohort.getSamples().size() != storageCohort.getSamples().size()) {
                            // Skip this cohort. This cohort should remain as invalid
                            logger.debug("Skip " + cohort.getId());
                            continue;
                        }

                        Set<String> cohortFromCatalog = cohort.getSamples()
                                .stream()
                                .map(Sample::getId)
                                .collect(Collectors.toSet());
                        Set<String> cohortFromStorage = storageCohort.getSamples()
                                .stream()
                                .map(id -> metadataManager.getSampleName(study.getId(), id))
                                .collect(Collectors.toSet());
                        if (!cohortFromCatalog.equals(cohortFromStorage)) {
                            // Skip this cohort. This cohort should remain as invalid
                            logger.debug("Skip " + cohort.getId());
                            continue;
                        }
                    }
                    if (cohort.getInternal().getStatus() == null || !cohort.getInternal().getStatus().getId().equals(CohortStatus.READY)) {
                        logger.debug("Cohort \"{}\" change status to {}", cohort.getId(), CohortStatus.READY);
                        catalogManager.getCohortManager().setStatus(study.getName(), cohort.getId(), CohortStatus.READY,
                                "Update status from Storage", sessionId);
                        modified = true;
                    }
                }
            }
        }

        Map<String, CohortMetadata> invalidStats = new HashMap<>();
        for (CohortMetadata cohortMetadata : metadataManager.getInvalidCohorts(study.getId())) {
            invalidStats.put(cohortMetadata.getName(), cohortMetadata);
        }
        //Check if any cohort stat has been invalidated
        if (!invalidStats.isEmpty()) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), invalidStats.keySet());
            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(study.getName(),
                    query,
                    COHORT_QUERY_OPTIONS, sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    if (cohort.getInternal().getStatus() == null || !cohort.getInternal().getStatus().getId().equals(CohortStatus.INVALID)) {
                        logger.debug("Cohort \"{}\" change status to {}", cohort.getId(), CohortStatus.INVALID);
                        catalogManager.getCohortManager().setStatus(study.getName(), cohort.getId(), CohortStatus.INVALID,
                                "Update status from Storage", sessionId);
                        modified = true;
                    }
                }
            }
        }
        return modified;
    }

    protected boolean synchronizeFiles(StudyMetadata study, List<File> files, String token) throws CatalogException {
        boolean modified = false;
        Map<String, Integer> fileNameMap = new HashMap<>();
        Map<Integer, String> filePathMap = new HashMap<>();
        Map<String, Set<String>> fileSamplesMap = new HashMap<>();
        LinkedHashSet<Integer> indexedFilesFromStorage = new LinkedHashSet<>();
        Set<String> annotationReadyFilesFromStorage = new HashSet<>();
        Set<String> secondaryIndexReadyFilesFromStorage = new HashSet<>();
        Set<Integer> allSamples = new HashSet<>();

        // -------------------------------------------------------------------
        logger.info("Read file metadata from Storage");
        // -------------------------------------------------------------------
        Iterable<FileMetadata> filesIterable;
        boolean fullSynchronize;
        if (CollectionUtils.isEmpty(files)) {
            fullSynchronize = true;
            filesIterable = () -> metadataManager.fileMetadataIterator(study.getId());
        } else {
            fullSynchronize = false;
            filesIterable = () -> files.stream()
                    .map(f -> metadataManager.getFileMetadata(study.getId(), f.getName()))
                    .filter(Objects::nonNull) // Prev line might return null values for files not in storage
                    .iterator();
        }
        for (FileMetadata fileMetadata : filesIterable) {
            fileNameMap.put(fileMetadata.getName(), fileMetadata.getId());
            filePathMap.put(fileMetadata.getId(), fileMetadata.getPath());
            Set<String> samples;
            if (fullSynchronize && !fileMetadata.isIndexed()) {
                // Skip non indexed files at full synchronize
                continue;
            }
            if (fileMetadata.isIndexed()) {
                indexedFilesFromStorage.add(fileMetadata.getId());
            }
            if (fileMetadata.getAnnotationStatus() == TaskMetadata.Status.READY) {
                annotationReadyFilesFromStorage.add(fileMetadata.getName());
            }
            if (fileMetadata.getSecondaryAnnotationIndexStatus() == TaskMetadata.Status.READY) {
                secondaryIndexReadyFilesFromStorage.add(fileMetadata.getName());
            }
            if (fileMetadata.getSamples() == null) {
                logger.warn("File '{}' with null samples", fileMetadata.getName());
                samples = Collections.emptySet();
                fileMetadata.setSamples(new LinkedHashSet<>());
                try {
                    VariantFileMetadata variantFileMetadata =
                            metadataManager.getVariantFileMetadata(study.getId(), fileMetadata.getId(), new QueryOptions()).first();
                    if (variantFileMetadata == null) {
                        logger.error("Missing VariantFileMetadata from file {}", fileMetadata.getName());
                    } else {
                        logger.info("Samples from VariantFileMetadata: {}", variantFileMetadata.getSampleIds());
                        // TODO: Should this case be filling the samples and fileMetadata.samples fields?
                    }
                } catch (StorageEngineException e) {
                    logger.error("Error reading VariantFileMetadata for file " + fileMetadata.getName(), e);
                }
            } else {
                samples = new HashSet<>(fileMetadata.getSamples().size());
                fileMetadata.getSamples().forEach(sid -> {
                    if (sid == null) {
                        logger.warn("File '{}' has a null sampleId in samples", fileMetadata.getName());
                    } else {
                        String name = metadataManager.getSampleName(study.getId(), sid);
                        samples.add(name);
                    }
                });
            }
            fileSamplesMap.put(fileMetadata.getName(), samples);
            allSamples.addAll(fileMetadata.getSamples());
        }

        if (!indexedFilesFromStorage.isEmpty()) {
            // -------------------------------------------------------------------
            logger.info("Synchronize {} catalog files from Storage", indexedFilesFromStorage.size());
            // -------------------------------------------------------------------
            List<String> indexedFilesUris = new ArrayList<>();
            for (Integer fileId : indexedFilesFromStorage) {
                String path = filePathMap.get(fileId);
                indexedFilesUris.add(toUri(path));
            }
            int batchSize = 2000;
            ProgressLogger progressLogger = new ProgressLogger("Synchronize files", indexedFilesUris.size()).setBatchSize(batchSize);
            int numFiles = 0;
            int modifiedFiles = 0;
            int notFoundFiles = 0;
            for (List<String> batch : BatchUtils.splitBatches(indexedFilesUris, batchSize, true)) {
                while (!batch.isEmpty()) {
                    int processedFilesInBatch = 0;
                    Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), batch);
                    try (DBIterator<File> iterator = catalogManager.getFileManager()
                            .iterator(study.getName(), query, INDEXED_FILES_QUERY_OPTIONS, token)) {
                        while (iterator.hasNext()) {
                            File file = iterator.next();
                            boolean annotationIndexReady = annotationReadyFilesFromStorage.contains(file.getName());
                            boolean secondaryIndexReady = secondaryIndexReadyFilesFromStorage.contains(file.getName());
                            if (synchronizeIndexedFile(study, file, fileSamplesMap, annotationIndexReady, secondaryIndexReady, token)) {
                                modifiedFiles++;
                                modified = true;
                            }

                            // Remove processed file from list of uris.
                            // Avoid double processing in case of exception
                            batch.remove(file.getUri().toString());
                            numFiles++;
                            processedFilesInBatch++;
                            progressLogger.increment(1, modifiedFiles + " updated files");
                        }

                        if (!batch.isEmpty()) {
                            notFoundFiles += batch.size();
                            logger.warn("Unable to find {} files in catalog: {}", batch.size(), batch);
                            // Discard not found files
                            batch.clear();
                        }
                    } catch (MongoServerException e) {
                        if (processedFilesInBatch == 0) {
                            // No files where processed in this loop. Do not continue.
                            throw e;
                        }
                        logger.warn("Catch exception " + e.toString() + ". Continue");
                    }
                }
            }
            if (numFiles != indexedFilesFromStorage.size()) {
                logger.warn("{} out of {} files were not found in catalog given their file uri",
                        indexedFilesFromStorage.size() - numFiles,
                        indexedFilesFromStorage.size());
            }
            if (notFoundFiles > 0) {
                logger.warn("Unable to find {} files from storage in catalog", notFoundFiles);
            }
        }


        // Update READY files
        // -------------------------------------------------------------------
        logger.info("Synchronize indexStatus=READY files up to Catalog");
        // -------------------------------------------------------------------
        Query indexedFilesQuery;
        if (CollectionUtils.isEmpty(files)) {
            indexedFilesQuery = INDEXED_FILES_QUERY;
        } else {
            List<String> catalogFileIds = files.stream()
                    .map(File::getId)
                    .collect(Collectors.toList());
            indexedFilesQuery = new Query(INDEXED_FILES_QUERY).append(ID.key(), catalogFileIds);
        }
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getName(), indexedFilesQuery, INDEXED_FILES_QUERY_OPTIONS, token)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Integer fileId = fileNameMap.get(file.getName());
                if (fileId == null || !indexedFilesFromStorage.contains(fileId)) {
                    String newStatus;
                    FileInternalVariantIndex index = file.getInternal().getVariant().getIndex();
                    if (index.hasTransform()) {
                        newStatus = VariantIndexStatus.TRANSFORMED;
                    } else {
                        newStatus = VariantIndexStatus.NONE;
                    }
                    logger.info("File \"{}\" change status from {} to {}", file.getName(),
                            VariantIndexStatus.READY, newStatus);
                    index.setStatus(new VariantIndexStatus(newStatus, "Not indexed, regarding Storage Metadata"));
                    catalogManager.getFileManager().updateFileInternalVariantIndex(file, index, token);
                    modified = true;
                }
            }
        }

        // Update ongoing files
        // -------------------------------------------------------------------
        logger.info("Synchronize indexStatus=INDEXING files up to Catalog");
        // -------------------------------------------------------------------
        Set<String> loadingFilesRegardingCatalog = new HashSet<>();
        Query runningIndexFilesQuery;
        if (CollectionUtils.isEmpty(files)) {
            runningIndexFilesQuery = RUNNING_INDEX_FILES_QUERY;
        } else {
            List<String> catalogFileIds = files.stream()
                    .map(File::getId)
                    .collect(Collectors.toList());
            runningIndexFilesQuery = new Query(RUNNING_INDEX_FILES_QUERY).append(ID.key(), catalogFileIds);
        }
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getName(), runningIndexFilesQuery, INDEXED_FILES_QUERY_OPTIONS, token)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Integer fileId = fileNameMap.get(file.getName());
                FileMetadata fileMetadata;
                if (fileId == null) {
                    fileMetadata = null;
                } else {
                    fileMetadata = metadataManager.getFileMetadata(study.getId(), fileId);
                }

                // If last LOAD operation is ERROR or there is no LOAD operation
                if (fileMetadata != null && fileMetadata.getIndexStatus().equals(TaskMetadata.Status.ERROR)) {
                    OpenCGAResult<Job> jobsFromFile = catalogManager
                            .getJobManager()
                            .search(study.getName(),
                                    new Query()
                                            .append(JobDBAdaptor.QueryParams.INPUT.key(), file.getId())
                                            .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), VariantIndexOperationTool.ID)
                                            .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Enums.ExecutionStatus.RUNNING),
                                    new QueryOptions(QueryOptions.INCLUDE, JobDBAdaptor.QueryParams.ID.key()),
                                    token);
                    if (jobsFromFile.getResults().isEmpty()) {
                        final FileInternalVariantIndex index;
                        index = file.getInternal().getVariant().getIndex() == null
                                ? FileInternalVariantIndex.init()
                                : file.getInternal().getVariant().getIndex();
                        String prevStatus = index.getStatus().getId();
                        String newStatus;
                        if (index.hasTransform()) {
                            newStatus = VariantIndexStatus.TRANSFORMED;
                        } else {
                            newStatus = VariantIndexStatus.NONE;
                        }
                        logger.info("File \"{}\" change status from {} to {}", file.getName(),
                                prevStatus, newStatus);
                        index.setStatus(new VariantIndexStatus(newStatus, "Error loading. Reset status to " + newStatus));

                        catalogManager.getFileManager().updateFileInternalVariantIndex(file, index, token);
                        modified = true;
                    } else {
                        // Running job. Might be transforming, or have just started. Do not modify the status!
                        loadingFilesRegardingCatalog.add(file.getName());
                    }
                } else {
                    loadingFilesRegardingCatalog.add(file.getName());
                }
            }
        }

        // Update running LOAD operations, regarding storage
        Set<String> loadingFilesRegardingStorage = new HashSet<>();
        for (TaskMetadata runningTask : metadataManager.getRunningTasks(study.getId())) {
            if (runningTask.getType().equals(TaskMetadata.Type.LOAD)
                    && runningTask.currentStatus() != null
                    && runningTask.currentStatus().equals(TaskMetadata.Status.RUNNING)) {
                for (Integer fileId : runningTask.getFileIds()) {
                    String filePath = filePathMap.get(fileId);
                    if (filePath != null) {
                        loadingFilesRegardingStorage.add(toUri(filePath));
                    }
                }
            }
        }

        if (!loadingFilesRegardingStorage.isEmpty()) {
            try (DBIterator<File> iterator = catalogManager.getFileManager()
                    .iterator(study.getName(), new Query(URI.key(), loadingFilesRegardingStorage),
                            INDEXED_FILES_QUERY_OPTIONS, token)) {
                while (iterator.hasNext()) {
                    File file = iterator.next();
                    String newStatus;
                    FileInternalVariantIndex index = file.getInternal().getVariant().getIndex();
                    if (index.hasTransform()) {
                        newStatus = VariantIndexStatus.LOADING;
                    } else {
                        newStatus = VariantIndexStatus.INDEXING;
                    }
                    index.setStatus(new VariantIndexStatus(newStatus, "File is being loaded regarding Storage"));
                    catalogManager.getFileManager().updateFileInternalVariantIndex(file, index, token);
                    modified = true;
                }
            }
        }

        modified |= synchronizeSamples(study, allSamples, token);

        return modified;
    }

    private boolean synchronizeIndexedFile(StudyMetadata study, File file, Map<String, Set<String>> fileSamplesMap,
                                           boolean annotationIndexReady, boolean secondaryIndexReady, String token)
            throws CatalogException {
        boolean modified = false;
        String status = FileInternal.getVariantIndexStatusId(file.getInternal());
        if (!status.equals(VariantIndexStatus.READY)) {
            final FileInternalVariantIndex index;
            index = file.getInternal().getVariant() == null || file.getInternal().getVariant().getIndex() == null
                    ? FileInternalVariantIndex.init() : file.getInternal().getVariant().getIndex();
            if (index.getStatus() == null) {
                index.setStatus(new VariantIndexStatus());
            }
            logger.debug("File \"{}\" change status from {} to {}", file.getName(), status, VariantIndexStatus.READY);
            index.setStatus(new VariantIndexStatus(VariantIndexStatus.READY, "Indexed, regarding Storage Metadata"));

            catalogManager.getFileManager().updateFileInternalVariantIndex(file, index, token);
            modified = true;
        }
        boolean catalogAnnotationIndexReady = InternalStatus.READY.equals(
                secureGet(file, f -> f.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null));
        if (annotationIndexReady != catalogAnnotationIndexReady) {
            FileInternalVariantAnnotationIndex internalVariantAnnotationIndex = FileInternalVariantAnnotationIndex.init();
            if (annotationIndexReady) {
                internalVariantAnnotationIndex.setStatus(new IndexStatus(IndexStatus.READY, "Annotation index completed"));
            } else {
                internalVariantAnnotationIndex.setStatus(new IndexStatus(IndexStatus.NONE, ""));
            }
            catalogManager.getFileManager().updateFileInternalVariantAnnotationIndex(file, internalVariantAnnotationIndex, token);
            modified = true;
        }
        boolean catalogSecondaryIndexReady = InternalStatus.READY.equals(
                secureGet(file, f -> f.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId(), null));
        if (secondaryIndexReady != catalogSecondaryIndexReady) {
            FileInternalVariantSecondaryAnnotationIndex internalVariantSecondaryIndex = FileInternalVariantSecondaryAnnotationIndex.init();
            if (secondaryIndexReady) {
                internalVariantSecondaryIndex.setStatus(new IndexStatus(IndexStatus.READY, "Secondary index completed"));
            } else {
                internalVariantSecondaryIndex.setStatus(new IndexStatus(IndexStatus.NONE, ""));
            }
            catalogManager.getFileManager().updateFileInternalVariantSecondaryAnnotationIndex(file, internalVariantSecondaryIndex, token);
            modified = true;
        }

        Set<String> storageSamples = fileSamplesMap.get(file.getName());
        Set<String> catalogSamples = new HashSet<>(file.getSampleIds());
        if (storageSamples == null) {
            storageSamples = new HashSet<>();
            Integer fileId = metadataManager.getFileId(study.getId(), file.getName());
            for (Integer sampleId : metadataManager.getSampleIdsFromFileId(study.getId(), fileId)) {
                storageSamples.add(metadataManager.getSampleName(study.getId(), sampleId));
            }
        }
        if (!storageSamples.equals(catalogSamples)) {
            logger.warn("File samples does not match between catalog and storage for file '{}'. "
                    + "Update catalog variant file metadata", file.getPath());
            file = catalogManager.getFileManager().get(study.getName(), file.getId(), new QueryOptions(), token).first();
            new FileMetadataReader(catalogManager).updateMetadataInformation(study.getName(), file, token);
        }
        return modified;
    }

    private String toUri(String path) {
        String uri;
        if (path.startsWith("/")) {
            uri = "file://" + path;
        } else {
            uri = Paths.get(path).toUri().toString();
        }
        return uri;
    }

    private boolean synchronizeSamples(StudyMetadata study, Collection<Integer> samples, String token) throws CatalogException {
        boolean modified = false;
        int sampleIndexVersion = study.getSampleIndexConfigurationLatest().getVersion();
        int modifiedSamples = 0;
        int batchSize = 2000;
        ProgressLogger progressLogger = new ProgressLogger("Synchronizing samples", samples.size()).setBatchSize(batchSize);
        logger.info("Synchronize {} samples", samples.size());

        for (List<Integer> sampleIdsBatch : BatchUtils.splitBatches(new ArrayList<>(samples), batchSize)) {
            Map<String, SampleMetadata> sampleMetadataMap = new HashMap<>(sampleIdsBatch.size());

            for (Integer sampleId : sampleIdsBatch) {
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(study.getId(), sampleId);
                sampleMetadataMap.put(sampleMetadata.getName(), sampleMetadata);
            }
            try (DBIterator<Sample> iterator = catalogManager.getSampleManager()
                    .iterator(study.getName(), new Query(SampleDBAdaptor.QueryParams.ID.key(), new ArrayList<>(sampleMetadataMap.keySet())),
                            SAMPLE_QUERY_OPTIONS, token)) {
                while (iterator.hasNext()) {
                    Sample sample = iterator.next();
                    if (synchronizeSample(sampleMetadataMap.get(sample.getId()), sample, sampleIndexVersion, token)) {
                        modified = true;
                        modifiedSamples++;
                    }
                    progressLogger.increment(1, ". " + modifiedSamples + " updated samples");
                }
            }
        }
        logger.info("{} samples synchronized. {} updated samples", samples.size(), modifiedSamples);
        return modified;
    }

    private boolean synchronizeSample(SampleMetadata sampleMetadata, Sample sample, int lastSampleIndexVersion, String token)
            throws CatalogException {
        boolean modified = false;

        String catalogIndexStatus = secureGet(sample, s -> s.getInternal().getVariant().getIndex().getStatus().getId(), null);
        int catalogNumFiles = secureGet(sample, s -> s.getInternal().getVariant().getIndex().getNumFiles(), 0);
        boolean catalogMultiFile = secureGet(sample, s -> s.getInternal().getVariant().getIndex().isMultiFile(), false);
        if (!sampleMetadata.getIndexStatus().name().equals(catalogIndexStatus)
                || catalogNumFiles != sampleMetadata.getFiles().size()
                || catalogMultiFile != sampleMetadata.isMultiFileSample()) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantIndex(sample,
                            new SampleInternalVariantIndex(
                                    new IndexStatus(sampleMetadata.getIndexStatus().name()),
                                    sampleMetadata.getFiles().size(),
                                    sampleMetadata.isMultiFileSample()), token);
            modified = true;
        }
        String catalogAnnotationIndexStatus = secureGet(sample, s -> s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null);
        if (!sampleMetadata.getAnnotationStatus().name().equals(catalogAnnotationIndexStatus)) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantAnnotationIndex(sample,
                            new SampleInternalVariantAnnotationIndex(
                                    new IndexStatus(sampleMetadata.getAnnotationStatus().name())), token);
            modified = true;
        }

        String catalogSecondaryAnnotationIndexStatus = secureGet(sample,
                s -> s.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId(), null);
        if (!sampleMetadata.getSecondaryAnnotationIndexStatus().name().equals(catalogSecondaryAnnotationIndexStatus)) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantSecondaryAnnotationIndex(sample,
                            new SampleInternalVariantSecondaryAnnotationIndex(
                                    new IndexStatus(sampleMetadata.getSecondaryAnnotationIndexStatus().name())), token);
            modified = true;
        }

        SampleInternalVariantSecondarySampleIndex catalogVariantSecondarySampleIndex =
                secureGet(sample, s -> s.getInternal().getVariant().getSecondarySampleIndex(), null);
        if (catalogVariantSecondarySampleIndex == null) {
            catalogVariantSecondarySampleIndex = new SampleInternalVariantSecondarySampleIndex();
        }
        boolean catalogVariantSecondarySampleIndexModified = false;


        // Get last valid version from this sample
        List<Integer> sampleIndexVersions = sampleMetadata.getSampleIndexVersions();
        List<Integer> sampleIndexAnnotationVersions = sampleMetadata.getSampleIndexAnnotationVersions();
        Integer expectedSampleIndexVersion = CollectionUtils.intersection(sampleIndexVersions, sampleIndexAnnotationVersions)
                .stream()
                .max(Integer::compareTo)
                .orElse(null);
        int sampleIndexVersion;
        if (expectedSampleIndexVersion == null) {
            sampleIndexVersion = lastSampleIndexVersion;
        } else {
            sampleIndexVersion = expectedSampleIndexVersion;
        }

        String sampleIndexStatus = sampleMetadata.getSampleIndexStatus(sampleIndexVersion) == TaskMetadata.Status.READY
                && sampleMetadata.getSampleIndexAnnotationStatus(sampleIndexVersion) == TaskMetadata.Status.READY
                ? IndexStatus.READY
                : IndexStatus.NONE;
        String sampleIndexMessage = "SecondarySampleIndex is "
                + (sampleIndexStatus.equals(IndexStatus.NONE) ? "not ready" : "ready")
                + " with version=" + sampleIndexVersion
                + ", genotypeStatus=" + sampleMetadata.getSampleIndexStatus(sampleIndexVersion)
                + ", annotationStatus=" + sampleMetadata.getSampleIndexAnnotationStatus(sampleIndexVersion);
        String catalogSecondarySampleIndexStatus = secureGet(sample, s -> s.getInternal().getVariant()
                .getSecondarySampleIndex().getStatus().getId(), null);
        String catalogSecondarySampleIndexMessage = secureGet(sample, s -> s.getInternal().getVariant()
                .getSecondarySampleIndex().getStatus().getDescription(), null);
        if (!sampleIndexStatus.equals(catalogSecondarySampleIndexStatus)
                || !sampleIndexMessage.equals(catalogSecondarySampleIndexMessage)) {
            catalogVariantSecondarySampleIndex.setStatus(
                    new IndexStatus(sampleIndexStatus, sampleIndexMessage));
            catalogVariantSecondarySampleIndexModified = true;
        }

        Integer catalogSecondarySampleIndexVersion = secureGet(sample, s -> s.getInternal().getVariant().getSecondarySampleIndex().getVersion(), -1);

        if (!Objects.equals(expectedSampleIndexVersion, catalogSecondarySampleIndexVersion)) {
            catalogVariantSecondarySampleIndex.setVersion(expectedSampleIndexVersion);
            catalogVariantSecondarySampleIndexModified = true;
        }

        String sampleIndexFamilyStatus = sampleMetadata.getFamilyIndexStatus(sampleIndexVersion).name();
        String catalogSecondarySampleIndexFamilyStatus = secureGet(sample, s -> s.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId(), null);
        if (!sampleIndexFamilyStatus.equals(catalogSecondarySampleIndexFamilyStatus)) {
            String message = "Family Index is "
                    + (sampleIndexStatus.equals(IndexStatus.NONE) ? "not ready" : "ready")
                    + " with version=" + sampleIndexVersion;
            catalogVariantSecondarySampleIndex.setFamilyStatus(
                    new IndexStatus(sampleIndexFamilyStatus, message));
            catalogVariantSecondarySampleIndexModified = true;
        }

        if (catalogVariantSecondarySampleIndexModified) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantSecondarySampleIndex(sample, catalogVariantSecondarySampleIndex, token);
            modified = true;
        }
        return modified;
    }

    public void synchronizeRemovedStudyFromStorage(String study, String token) throws CatalogException {
        catalogManager.getCohortManager().update(study, StudyEntry.DEFAULT_COHORT,
                new CohortUpdateParams().setSamples(Collections.emptyList()),
                true,
                new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), "SET")),
                token);

        catalogManager.getCohortManager().setStatus(study, StudyEntry.DEFAULT_COHORT, CohortStatus.NONE,
                "Study has been removed from storage", token);

        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study, INDEXED_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, token)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                catalogManager.getFileManager().updateFileInternalVariantIndex(file, FileInternalVariantIndex.init()
                        .setStatus(new VariantIndexStatus(VariantIndexStatus.NONE)), token);
            }
        }
    }

    protected static <O, T> T secureGet(O object, Function<O, T> function, T defaultValue) {
        try {
            return function.apply(object);
        } catch (NullPointerException e) {
            return defaultValue;
        }
    }

    static <T> T secureGet(Supplier<T> supplier, T defaultValue) {
        try {
            return supplier.get();
        } catch (NullPointerException e) {
            return defaultValue;
        }
    }

}
