
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
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.INDEX;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.NAME;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.PATH;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStudyConfigurationFactory {

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.PATH.key(),
            INDEX.key(),
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

    protected static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationFactory.class);

    private final CatalogManager catalogManager;


    public CatalogStudyConfigurationFactory(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public StudyConfiguration getStudyConfiguration(
            String study, StudyConfigurationManager studyConfigurationManager, QueryOptions options) throws CatalogException {
        StudyConfiguration studyConfiguration = null;
        QueryOptions qOpts = new QueryOptions(options);

        if (studyConfigurationManager != null) {
            studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, qOpts).first();
        }
        return studyConfiguration;
    }

    public void updateCatalogFromStudyConfiguration(StudyConfiguration studyConfiguration, String sessionId)
            throws CatalogException {

        logger.info("Updating StudyConfiguration " + studyConfiguration.getStudyId());

        //Check if cohort ALL has been modified
        String cohortName = StudyEntry.DEFAULT_COHORT;
        Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
        if (cohortId != null && studyConfiguration.getCohorts().get(cohortId) != null) {
            Set<String> cohortFromStorage = studyConfiguration.getCohorts().get(cohortId)
                    .stream()
                    .map(studyConfiguration.getSampleIds().inverse()::get)
                    .collect(Collectors.toSet());
            Cohort defaultCohort = catalogManager.getCohortManager()
                    .get(studyConfiguration.getStudyName(), cohortName, null, sessionId).first();
            List<String> cohortFromCatalog = defaultCohort
                    .getSamples()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (cohortFromCatalog.size() != cohortFromStorage.size() || !cohortFromStorage.containsAll(cohortFromCatalog)) {
                if (defaultCohort.getStatus().getName().equals(Cohort.CohortStatus.CALCULATING)) {
                    String status;
                    if (studyConfiguration.getInvalidStats().contains(cohortId)) {
                        status = Cohort.CohortStatus.INVALID;
                    } else if (studyConfiguration.getCalculatedStats().contains(cohortId)) {
                        status = Cohort.CohortStatus.READY;
                    } else {
                        status = Cohort.CohortStatus.NONE;
                    }
                    catalogManager.getCohortManager().setStatus(studyConfiguration.getStudyName(), cohortName, status, null, sessionId);
                }
                catalogManager.getCohortManager().update(studyConfiguration.getStudyName(), cohortName,
                        new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), cohortFromStorage),
                        true, null, sessionId);
            }
        }

        //Check if any cohort stat has been updated
        if (!studyConfiguration.getCalculatedStats().isEmpty()) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), studyConfiguration.getCalculatedStats().stream()
                    .map(studyConfiguration.getCohortIds().inverse()::get)
                    .collect(Collectors.toList()));

            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(studyConfiguration.getStudyName(),
                    query,
                    new QueryOptions(), sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    if (cohort.getStatus() != null && cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
                        if (cohort.getSamples().size() != studyConfiguration.getCohorts().get(cohortId).size()) {
                            // Skip this cohort. This cohort should remain as invalid
                            System.out.println("Skip " + cohort.getId());
                            continue;
                        }

                        Set<String> cohortFromCatalog = cohort.getSamples()
                                .stream()
                                .map(Sample::getId)
                                .collect(Collectors.toSet());
                        Set<String> cohortFromStorage = studyConfiguration.getCohorts().get(cohortId)
                                .stream()
                                .map(studyConfiguration.getSampleIds().inverse()::get)
                                .collect(Collectors.toSet());
                        if (!cohortFromCatalog.equals(cohortFromStorage)) {
                            // Skip this cohort. This cohort should remain as invalid
                            System.out.println("Skip " + cohort.getId());
                            continue;
                        }
                    }
                    if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.READY)) {
                        logger.debug("Cohort \"{}\" change status from {} to {}",
                                cohort.getId(), cohort.getStats(), Cohort.CohortStatus.READY);
                        catalogManager.getCohortManager().setStatus(studyConfiguration.getStudyName(), cohort.getId(),
                                Cohort.CohortStatus.READY, "Update status from Storage", sessionId);
                    }
                }
            }
        }

        //Check if any cohort stat has been invalidated
        if (!studyConfiguration.getInvalidStats().isEmpty()) {
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), studyConfiguration.getInvalidStats().stream()
                    .map(studyConfiguration.getCohortIds().inverse()::get)
                    .collect(Collectors.toList()));
            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(studyConfiguration.getStudyName(),
                    query,
                    new QueryOptions(), sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
                        logger.debug("Cohort \"{}\" change status from {} to {}",
                                cohort.getId(), cohort.getStats(), Cohort.CohortStatus.INVALID);
                        catalogManager.getCohortManager().setStatus(studyConfiguration.getStudyName(), cohort.getId(),
                                Cohort.CohortStatus.INVALID, "Update status from Storage", sessionId);
                    }
                }
            }
        }

        if (!studyConfiguration.getIndexedFiles().isEmpty()) {
            Query query = new Query(FileDBAdaptor.QueryParams.NAME.key(), studyConfiguration.getIndexedFiles().stream()
                    .map(studyConfiguration.getFileIds().inverse()::get)
                    .collect(Collectors.toList()));

            try (DBIterator<File> iterator = catalogManager.getFileManager().iterator(studyConfiguration.getStudyName(),
                    query,
                    new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(NAME.key(), PATH.key(), INDEX.key())), sessionId)) {
                while (iterator.hasNext()) {
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
                        catalogManager.getFileManager().setFileIndex(studyConfiguration.getStudyName(), file.getPath(), index, sessionId);
                    }
                }
            }
        }

        // Update READY files
        Set<Integer> indexedFiles = studyConfiguration.getIndexedFiles();
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyConfiguration.getStudyName(), INDEXED_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Integer fileId = studyConfiguration.getFileIds().get(file.getName());
                if (fileId == null || !indexedFiles.contains(fileId)) {
                    String newStatus;
                    if (hasTransformedFile(file.getIndex())) {
                        newStatus = FileIndex.IndexStatus.TRANSFORMED;
                    } else {
                        newStatus = FileIndex.IndexStatus.NONE;
                    }
                    logger.info("File \"{}\" change status from {} to {}", file.getName(),
                            FileIndex.IndexStatus.READY, newStatus);
                    catalogManager.getFileManager()
                            .updateFileIndexStatus(file, newStatus, "Not indexed, regarding StudyConfiguration", sessionId);
                }
            }
        }

        // Update ongoing files
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyConfiguration.getStudyName(), RUNNING_INDEX_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                Integer storageFileId = studyConfiguration.getFileIds().get(file.getName());

                BatchFileOperation loadOperation = null;
                // Find last load operation
                for (int i = studyConfiguration.getBatches().size() - 1; i >= 0; i--) {
                    BatchFileOperation op = studyConfiguration.getBatches().get(i);
                    if (op.getType().equals(BatchFileOperation.Type.LOAD) && op.getFileIds().contains(storageFileId)) {
                        loadOperation = op;
                        // Found last operation over this file.
                        break;
                    }
                }

                // If last LOAD operation is ERROR or there is no LOAD operation
                if (loadOperation != null && loadOperation.getStatus().lastEntry().getValue().equals(BatchFileOperation.Status.ERROR)
                        || loadOperation == null) {
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
                }

            }
        }
    }

    public boolean hasTransformedFile(FileIndex index) {
        return index.getTransformedFile() != null && index.getTransformedFile().getId() > 0;
    }

}
