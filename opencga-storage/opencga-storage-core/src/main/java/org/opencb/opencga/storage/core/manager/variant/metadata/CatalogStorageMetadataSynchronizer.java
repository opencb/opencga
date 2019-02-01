
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

package org.opencb.opencga.storage.core.manager.variant.metadata;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.FileIndex;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.*;

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
            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY)
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    public static final Query RUNNING_INDEX_FILES_QUERY = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
            FileIndex.IndexStatus.LOADING,
            FileIndex.IndexStatus.INDEXING))
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    protected static Logger logger = LoggerFactory.getLogger(CatalogStorageMetadataSynchronizer.class);

    private final CatalogManager catalogManager;
    private final VariantStorageMetadataManager metadataManager;


    public CatalogStorageMetadataSynchronizer(CatalogManager catalogManager, VariantStorageMetadataManager metadataManager) {
        this.catalogManager = catalogManager;
        this.metadataManager = metadataManager;
    }

    public StudyMetadata getStudyMetadata(String study) throws CatalogException {
        return metadataManager.getStudyMetadata(study);
    }

    /**
     * Updates catalog metadata from storage study configuration.
     *
     * 1) Update cohort ALL
     * 2) Update cohort status (calculating / invalid)
     * 3) Update file status
     *
     *
     * @param study                 StudyMetadata
     * @param sessionId             User session id
     * @throws CatalogException     if there is an error with catalog
     */
    public void synchronizeCatalogFromStorage(StudyMetadata study, String sessionId)
            throws CatalogException {

        logger.info("Updating study " + study.getId());

        //Check if cohort ALL has been modified
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        CohortMetadata defaultCohortStorage = metadataManager.getCohortMetadata(study.getId(), defaultCohortName);
        if (defaultCohortStorage != null) {
            Set<String> cohortFromStorage = defaultCohortStorage.getSamples()
                    .stream()
                    .map(s -> metadataManager.getSampleName(study.getId(), s))
                    .collect(Collectors.toSet());
            Cohort defaultCohort = catalogManager.getCohortManager()
                    .get(study.getName(), defaultCohortName, null, sessionId).first();
            List<String> cohortFromCatalog = defaultCohort
                    .getSamples()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (cohortFromCatalog.size() != cohortFromStorage.size() || !cohortFromStorage.containsAll(cohortFromCatalog)) {
                if (defaultCohort.getStatus().getName().equals(Cohort.CohortStatus.CALCULATING)) {
                    String status;
                    if (defaultCohortStorage.isInvalid()) {
                        status = Cohort.CohortStatus.INVALID;
                    } else if (defaultCohortStorage.isStatsReady()) {
                        status = Cohort.CohortStatus.READY;
                    } else {
                        status = Cohort.CohortStatus.NONE;
                    }
                    catalogManager.getCohortManager().setStatus(study.getName(), defaultCohortName, status, null, sessionId);
                }
                catalogManager.getCohortManager().update(study.getName(), defaultCohortName,
                        new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), cohortFromStorage),
                        true, null, sessionId);
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
                    CohortMetadata storageCohort = calculatedStats.get(cohort.getName());
                    if (cohort.getStatus() != null && cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
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
                                .map(s -> metadataManager.getSampleName(study.getId(), s))
                                .collect(Collectors.toSet());
                        if (!cohortFromCatalog.equals(cohortFromStorage)) {
                            // Skip this cohort. This cohort should remain as invalid
                            logger.debug("Skip " + cohort.getId());
                            continue;
                        }
                    }
                    if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.READY)) {
                        logger.debug("Cohort \"{}\" change status from {} to {}",
                                cohort.getId(), cohort.getStats(), Cohort.CohortStatus.READY);
                        catalogManager.getCohortManager().setStatus(study.getName(), cohort.getId(),
                                Cohort.CohortStatus.READY, "Update status from Storage", sessionId);
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
                    if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
                        logger.debug("Cohort \"{}\" change status from {} to {}",
                                cohort.getId(), cohort.getStats(), Cohort.CohortStatus.INVALID);
                        catalogManager.getCohortManager().setStatus(study.getName(), cohort.getId(),
                                Cohort.CohortStatus.INVALID, "Update status from Storage", sessionId);
                    }
                }
            }
        }

        LinkedHashSet<Integer> indexedFiles = metadataManager.getIndexedFiles(study.getId());
        if (!indexedFiles.isEmpty()) {
            List<String> list = new ArrayList<>();
            for (Integer fileId : indexedFiles) {
                String path = metadataManager.getFileMetadata(study.getId(), fileId).getPath();
                list.add(Paths.get(path).toUri().toString());
            }
            Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), list);

            try (DBIterator<File> iterator = catalogManager.getFileManager().iterator(study.getName(),
                    query,
                    new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(NAME.key(), PATH.key(), INDEX.key())), sessionId)) {
                int numFiles = 0;
                while (iterator.hasNext()) {
                    numFiles++;
                    File file = iterator.next();
                    String status = file.getIndex() == null || file.getIndex().getStatus() == null
                            ? FileIndex.IndexStatus.NONE
                            : file.getIndex().getStatus().getName();
                    if (!status.equals(FileIndex.IndexStatus.READY)) {
                        final FileIndex index;
                        index = file.getIndex() == null ? new FileIndex() : file.getIndex();
                        if (index.getStatus() == null) {
                            index.setStatus(new FileIndex.IndexStatus());
                        }
                        logger.debug("File \"{}\" change status from {} to {}", file.getName(),
                                file.getIndex().getStatus().getName(), FileIndex.IndexStatus.READY);
                        index.getStatus().setName(FileIndex.IndexStatus.READY);
                        catalogManager.getFileManager().setFileIndex(study.getName(), file.getPath(), index, sessionId);
                    }
                }
                if (numFiles != indexedFiles.size()) {
                    logger.warn("Some files were not found in catalog given their file uri");
                }
            }
        }

        // Update READY files
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getName(), INDEXED_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                FileMetadata fileMetadata = metadataManager.getFileMetadata(study.getId(), file.getName());
                if (fileMetadata == null || !fileMetadata.isIndexed()) {
                    String newStatus;
                    if (hasTransformedFile(file.getIndex())) {
                        newStatus = FileIndex.IndexStatus.TRANSFORMED;
                    } else {
                        newStatus = FileIndex.IndexStatus.NONE;
                    }
                    logger.info("File \"{}\" change status from {} to {}", file.getName(),
                            FileIndex.IndexStatus.READY, newStatus);
                    catalogManager.getFileManager()
                            .updateFileIndexStatus(file, newStatus, "Not indexed, regarding Storage Metadata", sessionId);
                }
            }
        }

        Set<String> loadingFilesRegardingCatalog = new HashSet<>();

        // Update ongoing files
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getName(), RUNNING_INDEX_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                FileMetadata fileMetadata = metadataManager.getFileMetadata(study.getId(), file.getName());

                // If last LOAD operation is ERROR or there is no LOAD operation
                if (fileMetadata != null && fileMetadata.getIndexStatus().equals(TaskMetadata.Status.ERROR)) {
                    final FileIndex index;
                    index = file.getIndex() == null ? new FileIndex() : file.getIndex();
                    String prevStatus = index.getStatus().getName();
                    String newStatus;
                    if (hasTransformedFile(index)) {
                        newStatus = FileIndex.IndexStatus.TRANSFORMED;
                    } else {
                        newStatus = FileIndex.IndexStatus.NONE;
                    }
                    logger.info("File \"{}\" change status from {} to {}", file.getName(),
                            prevStatus, newStatus);
                    catalogManager.getFileManager().updateFileIndexStatus(file, newStatus,
                            "Error loading. Reset status to " + newStatus,
                            sessionId);
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
                    String fileName = metadataManager.getFileName(study.getId(), fileId);
                    if (!loadingFilesRegardingCatalog.contains(fileName)) {
                        loadingFilesRegardingStorage.add(fileName);
                    }
                }
            }
        }

        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study.getName(), new Query(NAME.key(), loadingFilesRegardingStorage),
                        INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                String newStatus;
                if (hasTransformedFile(file.getIndex())) {
                    newStatus = FileIndex.IndexStatus.LOADING;
                } else {
                    newStatus = FileIndex.IndexStatus.INDEXING;
                }
                catalogManager.getFileManager().updateFileIndexStatus(file, newStatus, "File is being loaded regarding Storage", sessionId);
            }
        }

    }

    public boolean hasTransformedFile(FileIndex index) {
        return index.getTransformedFile() != null && index.getTransformedFile().getId() > 0;
    }

}
