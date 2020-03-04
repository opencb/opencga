
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

package org.opencb.opencga.analysis.variant.metadata;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileIndex;
import org.opencb.opencga.core.models.file.FileIndex.IndexStatus;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ID;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.URI;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStorageMetadataSynchronizer {

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.PATH.key(),
            FileDBAdaptor.QueryParams.INDEX.key(),
            FileDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final Query INDEXED_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), IndexStatus.READY)
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    public static final Query RUNNING_INDEX_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(IndexStatus.LOADING, IndexStatus.INDEXING))
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

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
     * @param study                 StudyMetadata
     * @param files                 Files to update
     * @param sessionId             User session id
     * @return if there were modifications in catalog
     * @throws CatalogException     if there is an error with catalog
     */
    public boolean synchronizeCatalogFilesFromStorage(String study, List<File> files, String sessionId, QueryOptions fileQueryOptions)
            throws CatalogException, StorageEngineException {

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        boolean modified = false;
        if (studyMetadata != null) {
            // Update Catalog file and cohort status.
            modified = synchronizeCatalogFilesFromStorage(studyMetadata, files, sessionId);
            if (modified) {
                // Files updated. Reload files from catalog
                for (int i = 0; i < files.size(); i++) {
                    File file = files.get(i);
                    files.set(i, catalogManager.getFileManager().get(study, file.getId(), fileQueryOptions, sessionId).first());
                }
            }
        }
        return modified;
    }

    /**
     * Updates catalog metadata from storage metadata.
     *
     * @param study                 StudyMetadata
     * @param files                 Files to update
     * @param sessionId             User session id
     * @return if there were modifications in catalog
     * @throws CatalogException     if there is an error with catalog
     */
    public boolean synchronizeCatalogFilesFromStorage(StudyMetadata study, List<File> files, String sessionId)
            throws CatalogException {
        logger.info("Synchronizing study " + study.getName());

        if (files != null && files.isEmpty()) {
            files = null;
        }

        return synchronizeFiles(study, files, sessionId);
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
     *
     * 1) Update cohort ALL
     * 2) Update cohort status (calculating / invalid)
     * 3) Update file status
     *
     *
     * @param study                 StudyMetadata
     * @param sessionId             User session id
     * @return if there were modifications in catalog
     * @throws CatalogException     if there is an error with catalog
     */
    public boolean synchronizeCatalogStudyFromStorage(StudyMetadata study, String sessionId)
            throws CatalogException {
        logger.info("Synchronizing study " + study.getName());

        boolean modified = synchronizeCohorts(study, sessionId);

        modified |= synchronizeFiles(study, null, sessionId);

        return modified;
    }

    protected boolean synchronizeCohorts(StudyMetadata study, String sessionId) throws CatalogException {
        boolean modified = false;
        Map<Integer, String> sampleNameMap = new HashMap<>();
        metadataManager.sampleMetadataIterator(study.getId()).forEachRemaining(sampleMetadata -> {
            sampleNameMap.put(sampleMetadata.getId(), sampleMetadata.getName());
        });

        //Check if cohort ALL has been modified
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        CohortMetadata defaultCohortStorage = metadataManager.getCohortMetadata(study.getId(), defaultCohortName);
        if (defaultCohortStorage != null) {
            Set<String> cohortFromStorage = defaultCohortStorage.getSamples()
                    .stream()
                    .map(sampleNameMap::get)
                    .collect(Collectors.toSet());
            Cohort defaultCohort = catalogManager.getCohortManager()
                    .get(study.getName(), defaultCohortName, null, sessionId).first();
            List<String> cohortFromCatalog = defaultCohort
                    .getSamples()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (cohortFromCatalog.size() != cohortFromStorage.size() || !cohortFromStorage.containsAll(cohortFromCatalog)) {
                if (defaultCohort.getInternal().getStatus().getName().equals(CohortStatus.CALCULATING)) {
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
                QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), "SET"));
                catalogManager.getCohortManager().update(study.getName(), defaultCohortName,
                        new CohortUpdateParams().setSamples(new ArrayList<>(cohortFromStorage)),
                        true, options, sessionId);
                modified = true;
            }
        }

        Map<String, CohortMetadata> calculatedStats = new HashMap<>();
        for (CohortMetadata cohortMetadata : metadataManager.getCalculatedCohorts(study.getId())) {
            calculatedStats.put(cohortMetadata.getName(), cohortMetadata);
        }
        //Check if any cohort stat has been updated
        if (!calculatedStats.isEmpty()) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), calculatedStats.keySet());

            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(study.getName(),
                    query, new QueryOptions(), sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    CohortMetadata storageCohort = calculatedStats.get(cohort.getId());
                    if (cohort.getInternal().getStatus() != null && cohort.getInternal().getStatus().getName().equals(CohortStatus.INVALID)) {
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
//                                .map(s -> metadataManager.getSampleName(study.getId(), s))
                                .map(sampleNameMap::get)
                                .collect(Collectors.toSet());
                        if (!cohortFromCatalog.equals(cohortFromStorage)) {
                            // Skip this cohort. This cohort should remain as invalid
                            logger.debug("Skip " + cohort.getId());
                            continue;
                        }
                    }
                    if (cohort.getInternal().getStatus() == null || !cohort.getInternal().getStatus().getName().equals(CohortStatus.READY)) {
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
                    new QueryOptions(), sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    if (cohort.getInternal().getStatus() == null || !cohort.getInternal().getStatus().getName().equals(CohortStatus.INVALID)) {
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

    protected boolean synchronizeFiles(StudyMetadata study, List<File> files, String sessionId) throws CatalogException {
        boolean modified = false;
        BiMap<Integer, String> fileNameMap = HashBiMap.create();
        Map<Integer, String> filePathMap = new HashMap<>();
        LinkedHashSet<Integer> indexedFiles;
        if (CollectionUtils.isEmpty(files)) {
            metadataManager.fileMetadataIterator(study.getId()).forEachRemaining(fileMetadata -> {
                fileNameMap.put(fileMetadata.getId(), fileMetadata.getName());
                filePathMap.put(fileMetadata.getId(), fileMetadata.getPath());
            });
            indexedFiles = metadataManager.getIndexedFiles(study.getId());
        } else {
            indexedFiles = new LinkedHashSet<>();
            for (File file : files) {
                FileMetadata fileMetadata = metadataManager.getFileMetadata(study.getId(), file.getName());
                if (fileMetadata != null) {
                    fileNameMap.put(fileMetadata.getId(), fileMetadata.getName());
                    filePathMap.put(fileMetadata.getId(), fileMetadata.getPath());
                    if (fileMetadata.isIndexed()) {
                        indexedFiles.add(fileMetadata.getId());
                    }
                }
            }
        }

        if (!indexedFiles.isEmpty()) {
            List<String> list = new ArrayList<>();
            for (Integer fileId : indexedFiles) {
                String path = filePathMap.get(fileId);
                list.add(toUri(path));
            }
            Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), list);
            try (DBIterator<File> iterator = catalogManager.getFileManager()
                    .iterator(study.getName(), query, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
                int numFiles = 0;
                while (iterator.hasNext()) {
                    numFiles++;
                    File file = iterator.next();
                    String status = file.getIndex() == null || file.getIndex().getStatus() == null
                            ? IndexStatus.NONE
                            : file.getIndex().getStatus().getName();
                    if (!status.equals(IndexStatus.READY)) {
                        final FileIndex index;
                        index = file.getIndex() == null ? new FileIndex() : file.getIndex();
                        if (index.getStatus() == null) {
                            index.setStatus(new IndexStatus());
                        }
                        logger.debug("File \"{}\" change status from {} to {}", file.getName(),
                                file.getIndex().getStatus().getName(), IndexStatus.READY);
                        index.getStatus().setName(IndexStatus.READY);
                        catalogManager.getFileManager()
                                .updateFileIndexStatus(file, IndexStatus.READY, "Indexed, regarding Storage Metadata", sessionId);
                        modified = true;
                    }
                }
                if (numFiles != indexedFiles.size()) {
                    logger.warn("Some files were not found in catalog given their file uri");
                }
            }
        }

        // Update READY files
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
                .iterator(study.getName(), indexedFilesQuery, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Integer fileId = fileNameMap.inverse().get(file.getName());
                if (fileId == null || !indexedFiles.contains(fileId)) {
                    String newStatus;
                    if (hasTransformedFile(file.getIndex())) {
                        newStatus = IndexStatus.TRANSFORMED;
                    } else {
                        newStatus = IndexStatus.NONE;
                    }
                    logger.info("File \"{}\" change status from {} to {}", file.getName(),
                            IndexStatus.READY, newStatus);
                    catalogManager.getFileManager()
                            .updateFileIndexStatus(file, newStatus, "Not indexed, regarding Storage Metadata", sessionId);
                    modified = true;
                }
            }
        }

        Set<String> loadingFilesRegardingCatalog = new HashSet<>();

        // Update ongoing files
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
                .iterator(study.getName(), runningIndexFilesQuery, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Integer fileId = fileNameMap.inverse().get(file.getName());
                FileMetadata fileMetadata;
                if (fileId == null) {
                    fileMetadata = null;
                } else {
                    fileMetadata = metadataManager.getFileMetadata(study.getId(), fileId);
                }

                // If last LOAD operation is ERROR or there is no LOAD operation
                if (fileMetadata != null && fileMetadata.getIndexStatus().equals(TaskMetadata.Status.ERROR)) {
                    final FileIndex index;
                    index = file.getIndex() == null ? new FileIndex() : file.getIndex();
                    String prevStatus = index.getStatus().getName();
                    String newStatus;
                    if (hasTransformedFile(index)) {
                        newStatus = IndexStatus.TRANSFORMED;
                    } else {
                        newStatus = IndexStatus.NONE;
                    }
                    logger.info("File \"{}\" change status from {} to {}", file.getName(),
                            prevStatus, newStatus);
                    catalogManager.getFileManager().updateFileIndexStatus(file, newStatus,
                            "Error loading. Reset status to " + newStatus,
                            sessionId);
                    modified = true;
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
                            INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
                while (iterator.hasNext()) {
                    File file = iterator.next();
                    String newStatus;
                    if (hasTransformedFile(file.getIndex())) {
                        newStatus = IndexStatus.LOADING;
                    } else {
                        newStatus = IndexStatus.INDEXING;
                    }
                    catalogManager.getFileManager().updateFileIndexStatus(file, newStatus,
                            "File is being loaded regarding Storage", sessionId);
                    modified = true;
                }
            }
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

    public boolean hasTransformedFile(FileIndex index) {
        return index.getTransformedFile() != null && index.getTransformedFile().getId() > 0;
    }

    public void synchronizeRemovedStudyFromStorage(String study, String token) throws CatalogException {
        catalogManager.getCohortManager().update(study, StudyEntry.DEFAULT_COHORT,
                new CohortUpdateParams().setSamples(Collections.emptyList()),
                true,
                new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), "SET")),
                token);

        catalogManager.getCohortManager().setStatus(study, StudyEntry.DEFAULT_COHORT, CohortStatus.NONE,
                "Study has been removed from storage", token);


        String userId = catalogManager.getUserManager().getUserId(token);
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study, INDEXED_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, token)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                catalogManager.getFileManager().setFileIndex(study, file.getId(),
                        new FileIndex(userId, TimeUtils.getTime(), new IndexStatus(IndexStatus.NONE), -1, null, null, null), token);
            }
        }
    }
}
