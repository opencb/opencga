
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

package org.opencb.opencga.storage.core.manager.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
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
            FileDBAdaptor.QueryParams.SAMPLE_IDS.key()));
    public static final Query ALL_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), Arrays.asList(File.Bioformat.VARIANT));

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.ID.key(),
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.PATH.key()));
    public static final Query INDEXED_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY);

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

    private final ObjectMapper objectMapper;
    private QueryOptions options;


    public CatalogStudyConfigurationFactory(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        objectMapper = new ObjectMapper();
    }

    public StudyConfiguration getStudyConfiguration(long studyId, QueryOptions options, String sessionId) throws CatalogException {
        return getStudyConfiguration(studyId, null, options, sessionId);
    }

    public StudyConfiguration getStudyConfiguration(long studyId, StudyConfigurationManager studyConfigurationManager, QueryOptions options,
                                                    String sessionId) throws CatalogException {
        Study study = catalogManager.getStudy(studyId, STUDY_QUERY_OPTIONS, sessionId).first();
        StudyConfiguration studyConfiguration = null;
        QueryOptions qOpts = new QueryOptions(options);

        if (studyConfigurationManager != null) {
            studyConfiguration = studyConfigurationManager.getStudyConfiguration((int) studyId, qOpts).first();
        }
        studyConfiguration = fillStudyConfiguration(studyConfiguration, study, sessionId);

        return studyConfiguration;
    }

    private StudyConfiguration fillStudyConfiguration(StudyConfiguration studyConfiguration, Study study, String sessionId)
            throws CatalogException {
        long studyId = study.getId();
        boolean newStudyConfiguration = false;
        if (studyConfiguration == null) {
            studyConfiguration = new StudyConfiguration(0, "");
            newStudyConfiguration = true;
        }
        studyConfiguration.setStudyId((int) study.getId());
        long projectId = catalogManager.getProjectIdByStudyId(study.getId());
        String projectAlias = catalogManager.getProject(projectId, null, sessionId).first().getAlias();
        if (projectAlias.contains("@")) {
            // Already contains user in projectAlias
            studyConfiguration.setStudyName(projectAlias + ':' + study.getAlias());
        } else {
            String userId = catalogManager.getUserIdByProjectId(projectId);
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
            studyConfiguration.setAggregation(VariantSource.Aggregation.valueOf(
                    aggregatedType));
        } else {
            studyConfiguration.setAggregation(VariantSource.Aggregation.NONE);
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

        logger.debug("Get Files");
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyId, ALL_FILES_QUERY, ALL_FILES_QUERY_OPTIONS, sessionId)) {
            while (iterator.hasNext()) {
                File file = iterator.next();

                int fileId = (int) file.getId();
                studyConfiguration.getFileIds().forcePut(file.getName(), fileId);
                List<Integer> sampleIds = new ArrayList<>(file.getSamples().size());
                for (Sample sample : file.getSamples()) {
                    sampleIds.add(toIntExact(sample.getId()));
                }
                studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(sampleIds));


//            if (studyConfiguration.getIndexedFiles().contains(fileId) && file.getAttributes().containsKey("variantSource")) {
//                //attributes.variantSource.metadata.variantFileHeader
//                Object object = file.getAttributes().get("variantSource");
//                if (object instanceof Map) {
//                    Map variantSource = ((Map) object);
//                    object = variantSource.get("metadata");
//                    if (object instanceof Map) {
//                        Map metadata = (Map) object;
//                        if (metadata.containsKey(VariantFileUtils.VARIANT_FILE_HEADER)) {
//                            String variantFileHeader = metadata.get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
//                            studyConfiguration.getHeaders().put(fileId, variantFileHeader);
//                        }
//                    }
//                }
//            }
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

    public void updateStudyConfigurationFromCatalog(long studyId, StudyConfigurationManager studyConfigurationManager, String sessionId)
            throws CatalogException, StorageEngineException {
        studyConfigurationManager.lockAndUpdate((int) studyId,
                studyConfiguration -> getStudyConfiguration(studyId, studyConfigurationManager, new QueryOptions(), sessionId));
    }

    public void updateCatalogFromStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options, String sessionId)
            throws CatalogException {
        if (options == null) {
            options = this.options;
        }
        logger.info("Updating StudyConfiguration " + studyConfiguration.getStudyId());

        //Check if cohort ALL has been modified
        Integer cohortId = studyConfiguration.getCohortIds().get(StudyEntry.DEFAULT_COHORT);
        if (cohortId != null && studyConfiguration.getCohorts().get(cohortId) != null) {
            Set<Long> cohortFromStorage = studyConfiguration.getCohorts().get(cohortId)
                    .stream()
                    .map(i -> (long) i)
                    .collect(Collectors.toSet());
            List<Long> cohortFromCatalog = catalogManager.getCohort(cohortId.longValue(), null, sessionId).first()
                    .getSamples()
                    .stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (cohortFromCatalog.size() != cohortFromStorage.size() || !cohortFromStorage.containsAll(cohortFromCatalog)) {
                catalogManager.getCohortManager().update(cohortId.longValue(),
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
                    if (file.getIndex() == null || !file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                        final FileIndex index;
                        index = file.getIndex() == null ? new FileIndex() : file.getIndex();
                        logger.debug("File \"{}\":{} change status from {} to {}", file.getName(), file.getId(),
                                file.getIndex().getStatus().getName(), FileIndex.IndexStatus.READY);
                        index.getStatus().setName(FileIndex.IndexStatus.READY);
                        catalogManager.getFileManager().setFileIndex(file.getId(), index, sessionId);
                    }
                }
            }
        }

        // Update READY files
        Query query = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                        FileDBAdaptor.QueryParams.NAME.key(),
                        FileDBAdaptor.QueryParams.INDEX.key()));
        Set<Long> indexedFiles = new HashSet<>();
        studyConfiguration.getIndexedFiles().forEach((e) -> indexedFiles.add(e.longValue()));
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyConfiguration.getStudyId(), query, queryOptions, sessionId)) {
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
        query = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
                FileIndex.IndexStatus.LOADING,
                FileIndex.IndexStatus.INDEXING));
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(studyConfiguration.getStudyId(), query, queryOptions, sessionId)) {
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
