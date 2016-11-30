
/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.Math.toIntExact;
/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStudyConfigurationFactory {

    public static final QueryOptions ALL_FILES_QUERY_OPTIONS = new QueryOptions()
            .append("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.name", "projects.studies.files.path",
                    "projects.studies.files.sampleIds", "projects.studies.files.attributes.variantSource.metadata.variantFileHeader"));
    public static final Query ALL_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), Arrays.asList(File.Bioformat.VARIANT, File.Bioformat.ALIGNMENT));

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions()
            .append("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.name", "projects.studies.files.path"));
    public static final Query INDEXED_FILES_QUERY = new Query()
            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY);

    public static final QueryOptions SAMPLES_QUERY_OPTIONS = new QueryOptions("include",
            Arrays.asList("projects.studies.samples.id", "projects.studies.samples.name"));

    public static final Query COHORTS_QUERY = new Query();
    public static final QueryOptions COHORTS_QUERY_OPTIONS = new QueryOptions();

    public static final QueryOptions INVALID_COHORTS_QUERY_OPTIONS = new QueryOptions()
            .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Cohort.CohortStatus.INVALID)
            .append("include",
                    Arrays.asList("projects.studies.cohorts.name", "projects.studies.cohorts.id", "projects.studies.cohorts.status"));
    protected static Logger logger = LoggerFactory.getLogger(CatalogStudyConfigurationFactory.class);

    private final CatalogManager catalogManager;

    public static final String STUDY_CONFIGURATION_FIELD = "studyConfiguration";
    public static final QueryOptions STUDY_QUERY_OPTIONS = new QueryOptions("include", Arrays.asList(
            "projects.studies.id",
            "projects.studies.alias",
            "projects.studies.attributes." + STUDY_CONFIGURATION_FIELD,
            "projects.studies.attributes." + VariantStorageManager.Options.AGGREGATED_TYPE.key()
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
        String userId = catalogManager.getUserIdByProjectId(projectId);
        studyConfiguration.setStudyName(userId + "@" + projectAlias + ":" + study.getAlias());

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

        if (study.getAttributes().containsKey(VariantStorageManager.Options.AGGREGATED_TYPE.key())) {
            String aggregatedType = study.getAttributes().get(VariantStorageManager.Options.AGGREGATED_TYPE.key()).toString();
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
        QueryResult<File> files = catalogManager.getAllFiles(studyId, ALL_FILES_QUERY, ALL_FILES_QUERY_OPTIONS, sessionId);
        for (File file : files.getResult()) {

            int fileId = (int) file.getId();
            studyConfiguration.getFileIds().forcePut(file.getName(), fileId);
            List<Integer> sampleIds = new ArrayList<>(file.getSampleIds().size());
            for (Long sampleId : file.getSampleIds()) {
                sampleIds.add(toIntExact(sampleId));
            }
            studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(sampleIds));


            if (studyConfiguration.getIndexedFiles().contains(fileId) && file.getAttributes().containsKey("variantSource")) {
                //attributes.variantSource.metadata.variantFileHeader
                Object object = file.getAttributes().get("variantSource");
                if (object instanceof Map) {
                    Map variantSource = ((Map) object);
                    object = variantSource.get("metadata");
                    if (object instanceof Map) {
                        Map metadata = (Map) object;
                        if (metadata.containsKey(VariantFileUtils.VARIANT_FILE_HEADER)) {
                            String variantFileHeader = metadata.get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
                            studyConfiguration.getHeaders().put(fileId, variantFileHeader);
                        }
                    }
                }
            }
        }

        logger.debug("Get Samples");
        QueryResult<Sample> samples = catalogManager.getAllSamples(studyId, new Query(), SAMPLES_QUERY_OPTIONS, sessionId);

        for (Sample sample : samples.getResult()) {
            studyConfiguration.getSampleIds().forcePut(sample.getName(), (int) sample.getId());
        }

        logger.debug("Get Cohorts");
        QueryResult<Cohort> cohorts = catalogManager.getAllCohorts(studyId, COHORTS_QUERY, COHORTS_QUERY_OPTIONS, sessionId);

        for (Cohort cohort : cohorts.getResult()) {
            int cohortId = (int) cohort.getId();
            studyConfiguration.getCohortIds().forcePut(cohort.getName(), cohortId);
            List<Integer> sampleIds = new ArrayList<>(cohort.getSamples().size());
            for (Long sampleId : cohort.getSamples()) {
                sampleIds.add(toIntExact(sampleId));
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
            throws CatalogException, StorageManagerException {
        try (StudyConfigurationManager.LockCloseable lock = studyConfigurationManager.closableLockStudy((int) studyId)) {
            StudyConfiguration studyConfiguration = getStudyConfiguration(studyId, studyConfigurationManager, new QueryOptions(),
                    sessionId);
            studyConfigurationManager.updateStudyConfiguration(studyConfiguration, new QueryOptions());
        }
    }

    public void updateCatalogFromStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options, String sessionId)
            throws CatalogException {
        if (options == null) {
            options = this.options;
        }
        logger.info("Updating StudyConfiguration " + studyConfiguration.getStudyId());

        //Check if any cohort stat has been updated
        if (!studyConfiguration.getCalculatedStats().isEmpty()) {
            for (Cohort cohort : catalogManager.getAllCohorts(studyConfiguration.getStudyId(),
                    new Query(CohortDBAdaptor.QueryParams.ID.key(), new ArrayList<>(studyConfiguration.getCalculatedStats())),
                    new QueryOptions(), sessionId).getResult()) {
                if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.READY)) {
                    logger.debug("Cohort \"{}\":{} change status from {} to {}",
                            cohort.getName(), cohort.getId(), cohort.getStats(), Cohort.CohortStatus.READY);
                    catalogManager.getCohortManager().setStatus(String.valueOf(cohort.getId()), Cohort.CohortStatus.READY,
                            "Update status from Storage", sessionId);
                }
            }
        }

        //Check if any cohort stat has been invalidated
        if (!studyConfiguration.getInvalidStats().isEmpty()) {
            for (Cohort cohort : catalogManager.getAllCohorts(studyConfiguration.getStudyId(),
                    new Query(CohortDBAdaptor.QueryParams.ID.key(), new ArrayList<>(studyConfiguration.getInvalidStats())),
                    new QueryOptions(), sessionId).getResult()) {
                if (cohort.getStatus() == null || !cohort.getStatus().getName().equals(Cohort.CohortStatus.INVALID)) {
                    logger.debug("Cohort \"{}\":{} change status from {} to {}",
                            cohort.getName(), cohort.getId(), cohort.getStats(), Cohort.CohortStatus.INVALID);
                    catalogManager.getCohortManager().setStatus(String.valueOf(cohort.getId()), Cohort.CohortStatus.INVALID,
                            "Update status from Storage", sessionId);
                }
            }
        }

        if (!studyConfiguration.getIndexedFiles().isEmpty()) {
            for (File file : catalogManager.getAllFiles(studyConfiguration.getStudyId(),
                    new Query(FileDBAdaptor.QueryParams.ID.key(), new ArrayList<>(studyConfiguration.getIndexedFiles())),
                    new QueryOptions(), sessionId)
                    .getResult()) {
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

        // Update READY files
        Query query = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                        FileDBAdaptor.QueryParams.NAME.key(),
                        FileDBAdaptor.QueryParams.INDEX.key()));
        Set<Long> indexedFiles = new HashSet<>();
        studyConfiguration.getIndexedFiles().forEach((e) -> indexedFiles.add(e.longValue()));
        for (File file : catalogManager.getAllFiles(studyConfiguration.getStudyId(), query, queryOptions, sessionId).getResult()) {
            if (!indexedFiles.contains(file.getId())) {
                logger.info("File \"{}\":{} change status from {} to {}", file.getName(), file.getId(),
                        FileIndex.IndexStatus.READY, FileIndex.IndexStatus.TRANSFORMED);
                catalogManager.getFileManager().updateFileIndexStatus(file, FileIndex.IndexStatus.TRANSFORMED,
                        "Not indexed, regarding StudyConfiguration", sessionId);
            }
        }

        // Update ongoing files
        query = new Query(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
                FileIndex.IndexStatus.LOADING,
                FileIndex.IndexStatus.INDEXING));
        for (File file : catalogManager.getAllFiles(studyConfiguration.getStudyId(), query, queryOptions, sessionId).getResult()) {
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
                if (index.getTransformedFile() != null && index.getTransformedFile().getId() > 0) {
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
