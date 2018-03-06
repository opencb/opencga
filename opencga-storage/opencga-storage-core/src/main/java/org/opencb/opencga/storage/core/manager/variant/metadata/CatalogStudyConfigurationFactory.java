
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
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;
/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStudyConfigurationFactory {

    public static final QueryOptions ALL_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.ID.key(),
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.PATH.key(),
            FileDBAdaptor.QueryParams.FORMAT.key(),
            FileDBAdaptor.QueryParams.SAMPLE_IDS.key()));

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.ID.key(),
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.PATH.key(),
            FileDBAdaptor.QueryParams.INDEX.key()));
    public static final Query INDEXED_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY)
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    public static final Query RUNNING_INDEX_FILES_QUERY = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
            FileIndex.IndexStatus.LOADING,
            FileIndex.IndexStatus.INDEXING))
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF.toString(), File.Format.GVCF.toString()));

    public static final QueryOptions SAMPLES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            SampleDBAdaptor.QueryParams.ID.key(),
            SampleDBAdaptor.QueryParams.NAME.key()));

    public static final Query COHORTS_QUERY = new Query();
    public static final QueryOptions COHORTS_QUERY_OPTIONS = new QueryOptions();

    protected static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationFactory.class);

    private final CatalogManager catalogManager;

    public static final QueryOptions STUDY_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            StudyDBAdaptor.QueryParams.ID.key(),
            StudyDBAdaptor.QueryParams.ALIAS.key(),
            StudyDBAdaptor.QueryParams.ATTRIBUTES.key() + '.' + VariantStorageEngine.Options.AGGREGATED_TYPE.key()
    ));

    public CatalogStudyConfigurationFactory(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public StudyConfiguration getStudyConfiguration(long studyId, QueryOptions options, String sessionId) throws CatalogException {
        return getStudyConfiguration(studyId, null, null, options, sessionId);
    }

    public StudyConfiguration getStudyConfiguration(
            long studyId, List<Long> filesToIndex, StudyConfigurationManager studyConfigurationManager, QueryOptions options,
            String sessionId) throws CatalogException {
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), STUDY_QUERY_OPTIONS, sessionId).first();
        StudyConfiguration studyConfiguration = null;
        QueryOptions qOpts = new QueryOptions(options);

        if (studyConfigurationManager != null) {
            studyConfiguration = studyConfigurationManager.getStudyConfiguration((int) studyId, qOpts).first();
        }
        studyConfiguration = fillStudyConfiguration(studyConfiguration, study, filesToIndex, sessionId);

        return studyConfiguration;
    }

    private StudyConfiguration fillStudyConfiguration(StudyConfiguration studyConfiguration, Study study, List<Long> filesToIndex,
                                                      String sessionId) throws CatalogException {
        long studyId = study.getId();
        boolean newStudyConfiguration = false;
        if (studyConfiguration == null) {
            studyConfiguration = new StudyConfiguration(0, "");
            newStudyConfiguration = true;
        }
        studyConfiguration.setStudyId((int) study.getId());
        long projectId = catalogManager.getStudyManager().getProjectId(study.getId());
        String projectAlias = catalogManager.getProjectManager().get(String.valueOf((Long) projectId), null, sessionId).first().getAlias();
        if (projectAlias.contains("@")) {
            // Already contains user in projectAlias
            studyConfiguration.setStudyName(projectAlias + ':' + study.getAlias());
        } else {
            String userId = catalogManager.getProjectManager().getOwner(projectId);
            studyConfiguration.setStudyName(userId + '@' + projectAlias + ':' + study.getAlias());
        }

        fillNullMaps(studyConfiguration);

        //Clear maps
//        studyConfiguration.getIndexedFiles().clear();
//        studyConfiguration.getFileIds().clear();
//        studyConfiguration.getSamplesInFiles().clear();
//        studyConfiguration.getHeaders().clear();
//        studyConfiguration.getSampleIds().clear();
//        studyConfiguration.getCalculatedStats().clear();
//        studyConfiguration.getInvalidStats().clear();
//        studyConfiguration.getCohortIds().clear();
//        studyConfiguration.getCohorts().clear();

        Object aggregationObj = study.getAttributes().get(VariantStorageEngine.Options.AGGREGATED_TYPE.key());
        if (aggregationObj != null) {
            String aggregatedType = aggregationObj.toString();
            logger.debug("setting study aggregation to {}", aggregatedType);
            studyConfiguration.setAggregationStr(aggregatedType);
        } else {
            studyConfiguration.setAggregation(Aggregation.NONE);
        }
        logger.debug("studyConfiguration aggregation: {}", studyConfiguration.getAggregation());

        // DO NOT update "indexed files" list. This MUST be modified only by storage.
        // This field will never be modified from catalog to storage
        // *** Except if it is a new StudyConfiguration...
//        if (newStudyConfiguration) {
//            for (File file : catalogManager.getAllFiles(studyId, INDEXED_FILES_QUERY,
//                      INDEXED_FILES_QUERY_OPTIONS, sessionId).getResult()) {
//                studyConfiguration.getIndexedFiles().add((int) file.getId());
//            }
//        }

        if (filesToIndex != null && !filesToIndex.isEmpty()) {
            logger.debug("Get Files");
            Query filesQuery = new Query(FileDBAdaptor.QueryParams.ID.key(), new ArrayList<>(filesToIndex));
            try (DBIterator<File> iterator = catalogManager.getFileManager()
                    .iterator(studyId, filesQuery, ALL_FILES_QUERY_OPTIONS, sessionId)) {
                while (iterator.hasNext()) {
                    File file = iterator.next();

                    if (!file.getFormat().equals(File.Format.VCF) && !file.getFormat().equals(File.Format.GVCF)) {
                        throw new CatalogException("Unexpected file format " + file.getFormat());
                    }

                    int fileId = toIntExact(file.getId());
                    Integer prevId = studyConfiguration.getFileIds().forcePut(file.getName(), fileId);
                    if (prevId != null && prevId != fileId) {
                        if (studyConfiguration.getIndexedFiles().contains(prevId)) {
                            throw new CatalogException("Unable to index multiple files with the same file name: "
                                    + "FileName '" + file.getName() + "' with ids [" + prevId + ", " + file.getId() + "] ");
                        } else {
                            logger.warn("Replacing fileId for file '" + file.getName() + "'. Previous id: " + prevId
                                    + ", new id: " + file.getId());
                        }
                    }
                    List<Integer> sampleIds = new ArrayList<>(file.getSamples().size());
                    for (Sample sample : file.getSamples()) {
                        sampleIds.add(toIntExact(sample.getId()));
                    }
                    studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(sampleIds));
                }
            }
        }

        logger.debug("Get Samples");
        try (DBIterator<Sample> iterator = catalogManager.getSampleManager()
                .iterator(studyId, new Query(), SAMPLES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                Sample sample = iterator.next();
                studyConfiguration.getSampleIds().forcePut(sample.getName(), toIntExact(sample.getId()));
            }
        }


        logger.debug("Get Cohorts");
        try (DBIterator<Cohort> iterator = catalogManager.getCohortManager()
                .iterator(studyId, COHORTS_QUERY, COHORTS_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                Cohort cohort = iterator.next();
                int cohortId = (int) cohort.getId();
                studyConfiguration.getCohortIds().forcePut(cohort.getName(), cohortId);
                if (cohort.getName().equals(StudyEntry.DEFAULT_COHORT)) {
                    // Skip default cohort
                    // Members of this cohort are managed by storage
                    // Only register cohortId
                    studyConfiguration.getCohorts().putIfAbsent(cohortId, Collections.emptySet());
                    continue;
                }
                List<Integer> sampleIds = new ArrayList<>(cohort.getSamples().size());
                for (Sample sample : cohort.getSamples()) {
                    sampleIds.add(toIntExact(sample.getId()));
                }
                studyConfiguration.getCohorts().put(cohortId, new HashSet<>(sampleIds));
                if (cohort.getStatus().getName().equals(Cohort.CohortStatus.READY)) {
                    studyConfiguration.getCalculatedStats().add(cohortId);
                    studyConfiguration.getInvalidStats().remove(cohortId);
                } else if (cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
                    studyConfiguration.getCalculatedStats().remove(cohortId);
                    studyConfiguration.getInvalidStats().add(cohortId);
                } else { //CALCULATING || NONE
                    studyConfiguration.getCalculatedStats().remove(cohortId);
                    studyConfiguration.getInvalidStats().remove(cohortId);
                }
            }
        }

        return studyConfiguration;
    }

    private void fillNullMaps(StudyConfiguration studyConfiguration) {
        if (studyConfiguration.getFileIds() == null) {
            studyConfiguration.setFileIds(new HashMap<>());
        }
        if (studyConfiguration.getSamplesInFiles() == null) {
            studyConfiguration.setSamplesInFiles(new HashMap<>());
        }
        if (studyConfiguration.getSampleIds() == null) {
            studyConfiguration.setSampleIds(new HashMap<>());
        }
        if (studyConfiguration.getCohortIds() == null) {
            studyConfiguration.setCohortIds(new HashMap<>());
        }
        if (studyConfiguration.getCohorts() == null) {
            studyConfiguration.setCohorts(new HashMap<>());
        }
        if (studyConfiguration.getAttributes() == null) {
            studyConfiguration.setAttributes(new ObjectMap());
        }
    }

    public void updateStudyConfigurationFromCatalog(
            long studyId, List<Long> filesToIndex, StudyConfigurationManager studyConfigurationManager, String sessionId)
            throws CatalogException, StorageEngineException {
        studyConfigurationManager.lockAndUpdate((int) studyId,
                studyConfiguration -> getStudyConfiguration(studyId, filesToIndex, studyConfigurationManager,
                        new QueryOptions(), sessionId));
    }

    public void updateCatalogFromStudyConfiguration(StudyConfiguration studyConfiguration, String sessionId)
            throws CatalogException {

        logger.info("Updating StudyConfiguration " + studyConfiguration.getStudyId());

        //Check if cohort ALL has been modified
        Integer cohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
        if (cohortId != null && studyConfiguration.getCohorts().get(cohortId) != null) {
            Set<Long> cohortFromStorage = studyConfiguration.getCohorts().get(cohortId)
                    .stream()
                    .map(Number::longValue)
                    .collect(Collectors.toSet());
            Cohort defaultCohort = catalogManager.getCohortManager()
                    .get(String.valueOf(studyConfiguration.getStudyId()), String.valueOf(cohortId), null, sessionId).first();
            List<Long> cohortFromCatalog = defaultCohort
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
                    catalogManager.getCohortManager().setStatus(String.valueOf(cohortId), status, null, sessionId);
                }
                catalogManager.getCohortManager().update(String.valueOf(studyConfiguration.getStudyId()), String.valueOf(cohortId),
                        new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), cohortFromStorage),
                        null, sessionId);
            }
        }

        //Check if any cohort stat has been updated
        if (!studyConfiguration.getCalculatedStats().isEmpty()) {
            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(studyConfiguration.getStudyId(),
                    new Query(CohortDBAdaptor.QueryParams.ID.key(), new ArrayList<>(studyConfiguration.getCalculatedStats())),
                    new QueryOptions(), sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.READY)) {
                        logger.debug("Cohort \"{}\":{} change status from {} to {}",
                                cohort.getName(), cohort.getId(), cohort.getStats(), Cohort.CohortStatus.READY);
                        catalogManager.getCohortManager().setStatus(String.valueOf(cohort.getId()), Cohort.CohortStatus.READY,
                                "Update status from Storage", sessionId);
                    }
                }
            }
        }

        //Check if any cohort stat has been invalidated
        if (!studyConfiguration.getInvalidStats().isEmpty()) {
            try (DBIterator<Cohort> iterator = catalogManager.getCohortManager().iterator(studyConfiguration.getStudyId(),
                    new Query(CohortDBAdaptor.QueryParams.ID.key(), new ArrayList<>(studyConfiguration.getInvalidStats())),
                    new QueryOptions(), sessionId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
                        logger.debug("Cohort \"{}\":{} change status from {} to {}",
                                cohort.getName(), cohort.getId(), cohort.getStats(), Cohort.CohortStatus.INVALID);
                        catalogManager.getCohortManager().setStatus(String.valueOf(cohort.getId()), Cohort.CohortStatus.INVALID,
                                "Update status from Storage", sessionId);
                    }
                }
            }
        }

        if (!studyConfiguration.getIndexedFiles().isEmpty()) {
            try (DBIterator<File> iterator = catalogManager.getFileManager().iterator(studyConfiguration.getStudyId(),
                    new Query(FileDBAdaptor.QueryParams.ID.key(), new ArrayList<>(studyConfiguration.getIndexedFiles())),
                    new QueryOptions(), sessionId)) {
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
                        logger.debug("File \"{}\":{} change status from {} to {}", file.getName(), file.getId(),
                                file.getIndex().getStatus().getName(), FileIndex.IndexStatus.READY);
                        index.getStatus().setName(FileIndex.IndexStatus.READY);
                        catalogManager.getFileManager().setFileIndex(file.getId(), index, sessionId);
                    }
                }
            }
        }

        // Update READY files
        Set<Long> indexedFiles = new HashSet<>();
        studyConfiguration.getIndexedFiles().forEach((e) -> indexedFiles.add(e.longValue()));
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyConfiguration.getStudyId(), INDEXED_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                if (!indexedFiles.contains(file.getId())) {
                    String newStatus;
                    if (hasTransformedFile(file.getIndex())) {
                        newStatus = FileIndex.IndexStatus.TRANSFORMED;
                    } else {
                        newStatus = FileIndex.IndexStatus.NONE;
                    }
                    logger.info("File \"{}\":{} change status from {} to {}", file.getName(), file.getId(),
                            FileIndex.IndexStatus.READY, newStatus);
                    catalogManager.getFileManager()
                            .updateFileIndexStatus(file, newStatus, "Not indexed, regarding StudyConfiguration", sessionId);
                }
            }
        }

        // Update ongoing files
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyConfiguration.getStudyId(), RUNNING_INDEX_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();

                BatchFileOperation loadOperation = null;
                // Find last load operation
                for (int i = studyConfiguration.getBatches().size() - 1; i >= 0; i--) {
                    BatchFileOperation op = studyConfiguration.getBatches().get(i);
                    if (op.getType().equals(BatchFileOperation.Type.LOAD) && op.getFileIds().contains((int) file.getId())) {
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
                    logger.info("File \"{}\":{} change status from {} to {}", file.getName(), file.getId(),
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
