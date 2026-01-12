
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

import com.google.common.collect.Iterators;
import com.mongodb.MongoServerException;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
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
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.variant.InternalVariantOperationIndex;
import org.opencb.opencga.core.models.variant.OperationIndexStatus;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ID;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.URI;
import static org.opencb.opencga.core.common.TimeUtils.toDate;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogStorageMetadataSynchronizer {

    public static final QueryOptions INDEXED_FILES_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            FileDBAdaptor.QueryParams.ID.key(),
            FileDBAdaptor.QueryParams.NAME.key(),
            FileDBAdaptor.QueryParams.TYPE.key(),
            FileDBAdaptor.QueryParams.PATH.key(),
            FileDBAdaptor.QueryParams.URI.key(),
            FileDBAdaptor.QueryParams.SAMPLE_IDS.key(),
            FileDBAdaptor.QueryParams.INTERNAL.key(),
            FileDBAdaptor.QueryParams.STUDY_UID.key(),
            FileDBAdaptor.QueryParams.RELATED_FILES.key()));
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
    public static final Query INDEXED_SAMPLES_QUERY = new Query()
            .append(SampleDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), IndexStatus.READY);
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

    public void synchronizeProjectMetadataFromCatalog(String projectFqn, String token)
            throws CatalogException, StorageEngineException {
        final Project project = catalogManager.getProjectManager().get(projectFqn,
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                ProjectDBAdaptor.QueryParams.ORGANISM.key(), ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key(),
                                ProjectDBAdaptor.QueryParams.CELLBASE.key())),
                        token)
                .first();

        int release = project.getCurrentRelease();
        CellBaseConfiguration cellbase = project.getCellbase();
        ProjectOrganism organism = project.getOrganism();
        String scientificName = CellBaseUtils.toCellBaseSpeciesName(organism.getScientificName());

        metadataManager.updateProjectMetadata(projectMetadata -> {
            if (projectMetadata == null) {
                projectMetadata = new ProjectMetadata();
            }
            projectMetadata.setSpecies(scientificName);
            projectMetadata.setAssembly(organism.getAssembly());
            projectMetadata.setDataRelease(cellbase.getDataRelease());
            projectMetadata.setRelease(release);
            return projectMetadata;
        });
    }

    public void synchronizeCatalogProjectFromStorageByStudy(String studyFqn, String token) throws CatalogException {
        String projectFqn = catalogManager.getStudyManager().getProjectFqn(studyFqn);
        synchronizeCatalogProjectFromStorage(projectFqn, Collections.singletonList(studyFqn), token);
    }

    public void synchronizeCatalogProjectFromStorage(String projectFqn, String token) throws CatalogException {
        if (!metadataManager.exists()) {
            return;
        }
        synchronizeCatalogProjectFromStorage(projectFqn, metadataManager.getStudyNames(), token);
    }

    private void synchronizeCatalogProjectFromStorage(String projectFqn, List<String> studies, String token) throws CatalogException {

        if (!metadataManager.exists()) {
            return;
        }
        logger.info("Synchronize project '{}' from Storage", projectFqn);
        ProjectMetadata projectMetadata = metadataManager.getProjectMetadata();

        Project project = catalogManager.getProjectManager().get(projectFqn,
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                ProjectDBAdaptor.QueryParams.FQN.key(),
                                ProjectDBAdaptor.QueryParams.ORGANISM.key(),
                                ProjectDBAdaptor.QueryParams.INTERNAL.key(),
                                ProjectDBAdaptor.QueryParams.CELLBASE.key())),
                        token)
                .first();
        projectFqn = project.getFqn();

        synchronizeProjectAnnotationIndexStatus(projectFqn, token, project, projectMetadata);
        synchronizeProjectSecondaryAnnotationIndexStatus(projectFqn, token, project, projectMetadata);

        for (String studyName : studies) {
            synchronizeStudyFromStorage(token, studyName);
        }
    }

    private void synchronizeProjectSecondaryAnnotationIndexStatus(String projectFqn, String token, Project project, ProjectMetadata projectMetadata)
            throws CatalogException {
        String secondaryAnnotationIndexStatus = secureGet(() -> project.getInternal().getVariant()
                .getSecondaryAnnotationIndex().getStatus().getId(), null);
        SearchIndexMetadata searchIndexMetadata = projectMetadata.getSecondaryAnnotationIndex().getSearchIndexMetadataForQueries();
        OperationIndexStatus operationIndexStatus;
        if (searchIndexMetadata == null) {
            operationIndexStatus = new OperationIndexStatus(OperationIndexStatus.PENDING,
                    "Variant secondary annotation index operation pending. "
                            + " variantIndexTs = " + toDate(projectMetadata.getVariantIndexLastTimestamp())
                            + ", variantAnnotationIndexTs = " + toDate(projectMetadata.getAnnotationIndexLastUpdateStartTimestamp())
                            + ", variantIndexStatsTs = " + toDate(projectMetadata.getStatsLastEndTimestamp())
            );
        } else {
            SearchIndexMetadata.DataStatus dataStatus = searchIndexMetadata.getDataStatus();

            switch (dataStatus) {
                case OUT_OF_DATE:
                case EMPTY:
                    operationIndexStatus = new OperationIndexStatus(OperationIndexStatus.PENDING,
                            "Variant secondary annotation index operation pending. "
                                    + " variantIndexTs = " + toDate(projectMetadata.getVariantIndexLastTimestamp())
                                    + ", variantSecondaryAnnotationIndexTs = " + searchIndexMetadata.getLastUpdateDate()
                                    + ", variantAnnotationIndexTs = " + toDate(projectMetadata.getAnnotationIndexLastUpdateStartTimestamp())
                                    + ", variantIndexStatsTs = " + toDate(projectMetadata.getStatsLastEndTimestamp())
                    );
                    break;
                case READY:
                    operationIndexStatus = new OperationIndexStatus(OperationIndexStatus.READY, "");
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + dataStatus);
            }
        }

        if (!operationIndexStatus.getName().equals(secondaryAnnotationIndexStatus)) {
            logger.info("Update project '{}' secondary annotation index status to {}",
                    projectFqn, operationIndexStatus.getId());
            catalogManager.getProjectManager().setProjectInternalVariantSecondaryAnnotationIndex(projectFqn,
                    new InternalVariantOperationIndex(operationIndexStatus),
                    new QueryOptions(), token);
        }
    }

    public void synchronizeStudyFromStorage(String token, String studyName) throws CatalogException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyName);
        if (studyMetadata == null) {
            logger.info("Study '{}' not found in storage", studyName);
            return;
        }
        Study study = catalogManager.getStudyManager().get(studyMetadata.getName(), new QueryOptions(), token).first();
        String status = secureGet(study,
                s -> s.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId(), OperationIndexStatus.PENDING);
        StudyMetadata.SampleIndexConfigurationVersioned latest = studyMetadata.getSampleIndexConfigurationLatest(true);

        InternalVariantOperationIndex operationIndex = null;
        switch (latest.getStatus()) {
            case STAGING:
                if (!status.equals(OperationIndexStatus.PENDING)) {
                    operationIndex = new InternalVariantOperationIndex(new OperationIndexStatus(OperationIndexStatus.PENDING, ""));
                }
                break;
            case ACTIVE:
                if (!status.equals(OperationIndexStatus.READY)) {
                    operationIndex = new InternalVariantOperationIndex(new OperationIndexStatus(OperationIndexStatus.READY, ""));
                }
                // What if there are some samples missing their family index?
                break;
            case DEPRECATED:
            case REMOVED:
                logger.warn("Study '{}' secondary sample index configuration is deprecated or removed. "
                        + "Please, update the configuration to the latest version.", studyMetadata.getName());
                break;
        }
        if (operationIndex != null) {
            logger.info("Update study '{}' secondary sample index status to {}",
                    studyMetadata.getName(), operationIndex.getStatus().getId());
            catalogManager.getStudyManager().setStudyInternalVariantSecondarySampleIndex(
                    studyMetadata.getName(), operationIndex, new QueryOptions(), token);
        }
    }

    private void synchronizeProjectAnnotationIndexStatus(String projectFqn, String token, Project project, ProjectMetadata projectMetadata) throws CatalogException {
        String annotationIndexStatus = secureGet(() -> project.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null);
        TaskMetadata.Status storageAnnotationIndexStatus = projectMetadata.getAnnotationIndexStatus();
        OperationIndexStatus operationIndexStatus;
        switch (storageAnnotationIndexStatus) {
            case NONE:
                if (projectMetadata.getVariantIndexLastTimestamp() <= 0) {
                    // Variant index has never been run. Annotation index can not be run.
                    operationIndexStatus = new OperationIndexStatus(OperationIndexStatus.NONE,
                            "Variant annotation index can not be run. Variant index has never been run.");
                } else {
                    operationIndexStatus = new OperationIndexStatus(OperationIndexStatus.PENDING,
                            "Variant annotation index operation pending. "
                                    + " variantIndexTs = " + toDate(projectMetadata.getVariantIndexLastTimestamp())
                                    + ", variantAnnotationIndexTs = "
                                    + toDate(projectMetadata.getAnnotationIndexLastFullUpdateStartTimestamp())
                    );
                }
                break;
            case READY:
                operationIndexStatus = new OperationIndexStatus(storageAnnotationIndexStatus.name(), "");
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + storageAnnotationIndexStatus);
        }
        if (!operationIndexStatus.getName().equals(annotationIndexStatus)) {
            logger.info("Update project '{}' annotation index status to {}",
                    projectFqn, operationIndexStatus.getId());
            catalogManager.getProjectManager().setProjectInternalVariantAnnotationIndex(projectFqn,
                    new InternalVariantOperationIndex(operationIndexStatus),
                    new QueryOptions(), token);
        }
    }

    /**
     * Updates catalog metadata from storage metadata.
     *
     * @param study     StudyMetadata
     * @param files     Files to update
     * @param token     User token
     * @param synchronizeCohorts Synchronize cohorts
     * @return if there were modifications in catalog
     * @throws CatalogException if there is an error with catalog
     */
    public boolean synchronizeCatalogFilesFromStorage(String study, List<String> files, String token, boolean synchronizeCohorts)
            throws CatalogException {
        List<File> filesToUpdate = new ArrayList<>(files.size());
        for (String fileStr : files) {
            try {
                File file = catalogManager.getFileManager().get(study, fileStr,
                        INDEXED_FILES_QUERY_OPTIONS, token).first();
                filesToUpdate.add(file);
            } catch (CatalogException e) {
                logger.warn("Could not find file '{}' in study '{}'", fileStr, study, e);
            }
        }
        return synchronizeCatalogFilesFromStorage(study, filesToUpdate, synchronizeCohorts, token);
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
            throws CatalogException {

        boolean modified = synchronizeCatalogFilesFromStorage(study, files, false, sessionId);
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
     * @param studyFqn           Study FQN
     * @param files              Files to update
     * @param synchronizeCohorts Synchronize cohorts
     * @param sessionId          User session id
     * @return if there were modifications in catalog
     * @throws CatalogException if there is an error with catalog
     */
    public boolean synchronizeCatalogFilesFromStorage(String studyFqn, List<File> files, boolean synchronizeCohorts, String sessionId)
            throws CatalogException {
        synchronizeCatalogProjectFromStorageByStudy(studyFqn, sessionId);

        StudyMetadata study = metadataManager.getStudyMetadata(studyFqn);
        if (study == null) {
            return false;
        }
        // Update Catalog file and cohort status.
        logger.info("Synchronizing study " + study.getName());

        if (files != null && files.isEmpty()) {
            files = null;
        }
        boolean modified = synchronizeFiles(study, files, sessionId);

        if (synchronizeCohorts) {
            modified |= synchronizeCohorts(study, sessionId);
        }
        return modified;
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
        synchronizeCatalogProjectFromStorageByStudy(studyFqn, sessionId);

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
    public boolean synchronizeCatalogFromStorage(String project, List<String> studies, String token) throws CatalogException {
        boolean modified = false;
        synchronizeCatalogProjectFromStorage(project, token);
        if (CollectionUtils.isEmpty(studies)) {
            studies = metadataManager.getStudyNames();
        }
        for (String study : studies) {
            StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
            modified |= synchronizeCatalogStudyFromStorage(studyMetadata, token);
        }
        return modified;
    }

    public boolean synchronizeCatalogFromStorage(String study, String sessionId)
            throws CatalogException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        if (studyMetadata != null) {
            synchronizeCatalogProjectFromStorageByStudy(studyMetadata.getName(), sessionId);
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
    private boolean synchronizeCatalogStudyFromStorage(StudyMetadata study, String sessionId)
            throws CatalogException {
        logger.info("Synchronizing study " + study.getName());

        boolean modified = synchronizeFiles(study, null, sessionId);

        modified |= synchronizeCohorts(study, sessionId);

        return modified;
    }

    private boolean synchronizeCohorts(StudyMetadata study, String sessionId) throws CatalogException {
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

                List<String> extraSamplesInCatalogCohort = new LinkedList<>(cohortFromCatalog);
                extraSamplesInCatalogCohort.removeAll(cohortFromStorage);
                List<String> missingSamplesInCatalogCohort = new LinkedList<>(cohortFromStorage);
                missingSamplesInCatalogCohort.removeAll(cohortFromCatalog);

                for (List<String> samplesToRemove : BatchUtils.splitBatches(extraSamplesInCatalogCohort, 100)) {
                    QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                            ParamUtils.BasicUpdateAction.REMOVE));
                    List<SampleReferenceParam> samples = samplesToRemove.stream()
                            .map(s -> new SampleReferenceParam().setId(s))
                            .collect(Collectors.toList());
                    catalogManager.getCohortManager().update(study.getName(), defaultCohortName,
                            new CohortUpdateParams().setSamples(samples),
                            true, options, sessionId);
                }
                ProgressLogger progressLogger = new ProgressLogger("Add samples to cohort " + defaultCohortName,
                        missingSamplesInCatalogCohort.size());
                for (List<String> samplesToAdd : BatchUtils.splitBatches(missingSamplesInCatalogCohort, 100)) {
                    QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                            ParamUtils.BasicUpdateAction.ADD));
                    List<SampleReferenceParam> samples = samplesToAdd.stream()
                            .map(s -> new SampleReferenceParam().setId(s))
                            .collect(Collectors.toList());
                    catalogManager.getCohortManager().update(study.getName(), defaultCohortName,
                            new CohortUpdateParams().setSamples(samples),
                            true, options, sessionId);
                    progressLogger.increment(samplesToAdd.size());
                }
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

    private boolean synchronizeSampleIndexConfiguration(StudyMetadata studyMetadata, String token) throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyMetadata.getName(), new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION.key())), token).first();
        SampleIndexConfiguration sampleIndexFromCatalog = study.getInternal().getConfiguration().getVariantEngine().getSampleIndex();
        SampleIndexConfiguration sampleIndexFromStorage = studyMetadata.getSampleIndexConfigurationLatest().getConfiguration();
        if (!sampleIndexFromStorage.equals(sampleIndexFromCatalog)) {
            logger.info("Update sample index configuration from storage for study {}", studyMetadata.getName());
            catalogManager.getStudyManager().setVariantEngineConfigurationSampleIndex(studyMetadata.getName(), sampleIndexFromStorage, token);
            return true;
        } else {
            return false;
        }
    }

    protected boolean synchronizeFiles(StudyMetadata study, List<File> files, String token) throws CatalogException {
        boolean modified = false;

        // FIXME: This method call should be relocated
        modified |= synchronizeSampleIndexConfiguration(study, token);

        Map<URI, Integer> fileURIMap = new HashMap<>();
        Map<Integer, String> filePathMap = new HashMap<>();
        Set<Integer> virtualFiles = new HashSet<>();
        Map<URI, Set<String>> fileSamplesMap = new HashMap<>();
        LinkedHashSet<Integer> indexedFilesFromStorage = new LinkedHashSet<>();
        Set<URI> annotationReadyFilesFromStorage = new HashSet<>();
        Set<URI> secondaryIndexReadyFilesFromStorage = new HashSet<>();
        Set<Integer> allSamples = new HashSet<>();

        // -------------------------------------------------------------------
        logger.info("Read file metadata from Storage" + (CollectionUtils.isEmpty(files) ? " for all files" : " for " + files.size() + " files"));
        // -------------------------------------------------------------------
        Iterable<FileMetadata> filesIterable;
        boolean fullSynchronize;
        if (CollectionUtils.isEmpty(files)) {
            fullSynchronize = true;
            filesIterable = () -> metadataManager.fileMetadataIterator(study.getId());
        } else {
            fullSynchronize = false;
            filesIterable = () -> {
                Iterator<FileMetadata> iteratorMain = files.stream()
                        .map(f -> {
                            FileMetadata fm = metadataManager.getFileMetadata(study.getId(), VariantCatalogQueryUtils.toStorageFileName(f));
                            if (fm != null) {
                                if (fm.getType() == FileMetadata.Type.PARTIAL) {
                                    virtualFiles.add(fm.getAttributes().getInt(FileMetadata.VIRTUAL_PARENT));
                                }
                            }
                            return fm;
                        })
                        .filter(Objects::nonNull) // Prev line might return null values for files not in storage
                        .iterator();
                Iterator<FileMetadata> iteratorVirtual = virtualFiles.stream()
                        .map(fid -> metadataManager.getFileMetadata(study.getId(), fid))
                        .filter(Objects::nonNull) // Prev line might return null values for files not in storage
                        .iterator();
                return Iterators.concat(iteratorMain, iteratorVirtual);
            };
        }
        for (FileMetadata fileMetadata : filesIterable) {
            fileURIMap.put(fileMetadata.getURI(), fileMetadata.getId());
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
                annotationReadyFilesFromStorage.add(fileMetadata.getURI());
            }
            if (fileMetadata.getSecondaryAnnotationIndexStatus() == TaskMetadata.Status.READY) {
                secondaryIndexReadyFilesFromStorage.add(fileMetadata.getURI());
            }
            if (fileMetadata.getSamples() == null) {
                logger.warn("File '{}' with null samples", fileMetadata.getName());
                samples = Collections.emptySet();
                fileMetadata.setSamples(new LinkedHashSet<>());
                try {
                    VariantFileMetadata variantFileMetadata =
                            metadataManager.getVariantFileMetadataOrNull(study.getId(), fileMetadata.getId());
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
            fileSamplesMap.put(fileMetadata.getURI(), samples);
            allSamples.addAll(fileMetadata.getSamples());
            if (samples.size() > 100) {
                // Try to reuse value.
                // If the file holds more than 100 samples, it's most likely this same set of samples is already present
                for (Set<String> value : fileSamplesMap.values()) {
                    if (value.equals(samples)) {
                        fileSamplesMap.put(fileMetadata.getURI(), value);
                        break;
                    }
                }
            }
        }

        if (!indexedFilesFromStorage.isEmpty()) {
            // -------------------------------------------------------------------
            logger.info("Synchronize {} catalog files from Storage", indexedFilesFromStorage.size());
            // -------------------------------------------------------------------

            for (Integer virtualFile : virtualFiles) {
                File file = catalogManager.getFileManager()
                        .get(study.getName(), filePathMap.get(virtualFile), INDEXED_FILES_QUERY_OPTIONS, token).first();
                boolean annotationIndexReady = annotationReadyFilesFromStorage.contains(getFileUri(file));
                boolean secondaryIndexReady = secondaryIndexReadyFilesFromStorage.contains(getFileUri(file));
                if (synchronizeIndexedFile(study, file, fileSamplesMap, annotationIndexReady, secondaryIndexReady, token, true)) {
                    modified = true;
                }
            }

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
                            boolean annotationIndexReady = annotationReadyFilesFromStorage.contains(getFileUri(file));
                            boolean secondaryIndexReady = secondaryIndexReadyFilesFromStorage.contains(getFileUri(file));
                            if (synchronizeIndexedFile(study, file, fileSamplesMap, annotationIndexReady, secondaryIndexReady, token, true)) {
                                modifiedFiles++;
                                modified = true;
                            }

                            // Remove processed file from list of uris.
                            // Avoid double processing in case of exception
                            batch.remove(getFileUri(file).toString());
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
                Integer fileId = fileURIMap.get(getFileUri(file));
                if (fileId == null || !indexedFilesFromStorage.contains(fileId)) {
                    // Check for annotation index and secondary annotation index
                    boolean annotationIndexReady;
                    boolean secondaryIndexReady;
                    if (fileId == null) {
                        annotationIndexReady = false;
                        secondaryIndexReady = false;
                    } else {
                        annotationIndexReady = annotationReadyFilesFromStorage.contains(getFileUri(file));
                        secondaryIndexReady = secondaryIndexReadyFilesFromStorage.contains(getFileUri(file));
                    }
                    synchronizeIndexedFile(study, file, fileSamplesMap, annotationIndexReady, secondaryIndexReady, token, false);
                    modified = true;
                }
            }
        }

        // Update ongoing files
        // -------------------------------------------------------------------
        logger.info("Synchronize indexStatus=INDEXING files up to Catalog");
        // -------------------------------------------------------------------
        Set<URI> loadingFilesRegardingCatalog = new HashSet<>();
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
                Integer fileId = fileURIMap.get(getFileUri(file));
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
                        logger.info("File \"{}\" change status from {} to {}", file.getId(),
                                prevStatus, newStatus);
                        index.setStatus(new VariantIndexStatus(newStatus, "Error loading. Reset status to " + newStatus));

                        catalogManager.getFileManager().updateFileInternalVariantIndex(study.getName(), file, index, token);
                        modified = true;
                    } else {
                        // Running job. Might be transforming, or have just started. Do not modify the status!
                        loadingFilesRegardingCatalog.add(getFileUri(file));
                    }
                } else {
                    loadingFilesRegardingCatalog.add(getFileUri(file));
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
                    catalogManager.getFileManager().updateFileInternalVariantIndex(study.getName(), file, index, token);
                    modified = true;
                }
            }
        }

        modified |= synchronizeSamples(study, allSamples, token);

        return modified;
    }

    private static URI getFileUri(File file) {
        if (file.getType() == File.Type.VIRTUAL) {
            return UriUtils.toUri(file.getPath());
        } else {
            return file.getUri();
        }
    }

    private boolean synchronizeIndexedFile(StudyMetadata study, File file, Map<URI, Set<String>> fileSamplesMap,
                                           boolean annotationIndexReady, boolean secondaryIndexReady, String token, boolean mainIndexReady)
            throws CatalogException {
        boolean modified = false;
        String status = FileInternal.getVariantIndexStatusId(file.getInternal());
        boolean catalogMainIndexReady = status.equals(VariantIndexStatus.READY);
        if (catalogMainIndexReady != mainIndexReady) {
            final FileInternalVariantIndex index;
            index = file.getInternal().getVariant() == null || file.getInternal().getVariant().getIndex() == null
                    ? FileInternalVariantIndex.init() : file.getInternal().getVariant().getIndex();
            if (index.getStatus() == null) {
                index.setStatus(new VariantIndexStatus());
            }
            if (mainIndexReady) {
                logger.debug("File \"{}\" change status from {} to {}", file.getName(), status, VariantIndexStatus.READY);
                index.setStatus(new VariantIndexStatus(VariantIndexStatus.READY, "Indexed, regarding Storage Metadata"));
            } else {
                String newStatus;
                if (index.hasTransform()) {
                    newStatus = VariantIndexStatus.TRANSFORMED;
                } else {
                    newStatus = VariantIndexStatus.NONE;
                }
                logger.info("File \"{}\" change status from {} to {}", file.getName(),
                        VariantIndexStatus.READY, newStatus);
                index.setStatus(new VariantIndexStatus(newStatus, "Not indexed, regarding Storage Metadata"));
            }
            catalogManager.getFileManager().updateFileInternalVariantIndex(study.getName(), file, index, token);
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
            catalogManager.getFileManager().updateFileInternalVariantAnnotationIndex(study.getName(), file, internalVariantAnnotationIndex, token);
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
            catalogManager.getFileManager().updateFileInternalVariantSecondaryAnnotationIndex(study.getName(), file, internalVariantSecondaryIndex, token);
            modified = true;
        }

        Set<String> storageSamples = fileSamplesMap.get(getFileUri(file));
        Set<String> catalogSamples = new HashSet<>(file.getSampleIds());
        if (storageSamples == null) {
            storageSamples = new HashSet<>();
            Integer fileId = metadataManager.getFileId(study.getId(), VariantCatalogQueryUtils.toStorageFileName(file));
            for (Integer sampleId : metadataManager.getSampleIdsFromFileId(study.getId(), fileId)) {
                storageSamples.add(metadataManager.getSampleName(study.getId(), sampleId));
            }
        }
        if (!storageSamples.equals(catalogSamples) && !FileUtils.isPartial(file)) {
            logger.warn("File samples does not match between catalog and storage for file '{}'. "
                    + "Update catalog variant file metadata", file.getPath());
            file = catalogManager.getFileManager().get(study.getName(), file.getId(), new QueryOptions(), token).first();
            new FileMetadataReader(catalogManager).updateMetadataInformation(study.getName(), file, token);
        }
        return modified;
    }

    private String toUri(String path) {
        return Paths.get(path).toUri().toString();
    }

    private boolean synchronizeSamples(StudyMetadata study, Collection<Integer> samples, String token) throws CatalogException {
        boolean modified = false;
        int sampleIndexVersion = study.getSampleIndexConfigurationLatest().getVersion();
        int modifiedSamples = 0;
        int batchSize = 2000;
        ProgressLogger progressLogger = new ProgressLogger("Synchronizing samples", samples.size());
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
                    if (synchronizeSample(study, sampleMetadataMap.get(sample.getId()), sample, sampleIndexVersion, token)) {
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

    private boolean synchronizeSample(StudyMetadata study, SampleMetadata sampleMetadata, Sample sample, int lastSampleIndexVersion,
                                      String token) throws CatalogException {
        boolean modified = false;

        String catalogIndexStatus = secureGet(sample, s -> s.getInternal().getVariant().getIndex().getStatus().getId(), null);
        int catalogNumFiles = secureGet(sample, s -> s.getInternal().getVariant().getIndex().getNumFiles(), 0);
        boolean catalogMultiFile = secureGet(sample, s -> s.getInternal().getVariant().getIndex().isMultiFile(), false);
        if (!sampleMetadata.getIndexStatus().name().equals(catalogIndexStatus)
                || catalogNumFiles != sampleMetadata.getFiles().size()
                || catalogMultiFile != sampleMetadata.isMultiFileSample()) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantIndex(study.getName(), sample,
                            new SampleInternalVariantIndex(
                                    toIndexStatus(sampleMetadata.getIndexStatus()),
                                    sampleMetadata.getFiles().size(),
                                    sampleMetadata.isMultiFileSample()), token);
            modified = true;
        }
        String catalogAnnotationIndexStatus = secureGet(sample, s -> s.getInternal().getVariant().getAnnotationIndex().getStatus().getId(), null);
        if (!sampleMetadata.getAnnotationStatus().name().equals(catalogAnnotationIndexStatus)) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantAnnotationIndex(study.getName(), sample,
                            new SampleInternalVariantAnnotationIndex(
                                    toIndexStatus(sampleMetadata.getAnnotationStatus())), token);
            modified = true;
        }

        String catalogSecondaryAnnotationIndexStatus = secureGet(sample,
                s -> s.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId(), null);
        if (!sampleMetadata.getSecondaryAnnotationIndexStatus().name().equals(catalogSecondaryAnnotationIndexStatus)) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantSecondaryAnnotationIndex(study.getName(), sample,
                            new SampleInternalVariantSecondaryAnnotationIndex(
                                    toIndexStatus(sampleMetadata.getSecondaryAnnotationIndexStatus())), token);
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
                + (sampleIndexStatus.equals(IndexStatus.READY) ? "ready" : "not ready")
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

        IndexStatus sampleIndexFamilyStatus = toIndexStatus(sampleMetadata.getFamilyIndexStatus(sampleIndexVersion));
        String catalogSecondarySampleIndexFamilyStatus = secureGet(sample, s -> s.getInternal().getVariant()
                .getSecondarySampleIndex().getFamilyStatus().getId(), null);
        if (!sampleIndexFamilyStatus.getId().equals(catalogSecondarySampleIndexFamilyStatus)) {
            String message = "Family Index is "
                    + (sampleIndexFamilyStatus.getId().equals(IndexStatus.READY) ? "ready" : "not ready")
                    + " with version=" + sampleIndexVersion;
            catalogVariantSecondarySampleIndex.setFamilyStatus(
                    new IndexStatus(sampleIndexFamilyStatus.getId(), message));
            catalogVariantSecondarySampleIndexModified = true;
        }

        if (catalogVariantSecondarySampleIndexModified) {
            catalogManager.getSampleManager()
                    .updateSampleInternalVariantSecondarySampleIndex(study.getName(), sample, catalogVariantSecondarySampleIndex, token);
            modified = true;
        }

        List<SampleInternalVariantAggregateFamily> aggregateFamilies = secureGet(sample,
                s -> s.getInternal().getVariant().getAggregateFamily(), Collections.emptyList());
        if (aggregateFamilies == null) {
            aggregateFamilies = Collections.emptyList();
        }
        if (!sampleMetadata.getInternalCohorts().isEmpty() || !aggregateFamilies.isEmpty()) {
            List<SampleInternalVariantAggregateFamily> aggregateFamiliesUpdated = new ArrayList<>();
            boolean aggregateFamilyModified = false;
            for (Integer internalCohort : sampleMetadata.getInternalCohorts()) {
                if (CohortMetadata.getType(metadataManager.getCohortName(study.getId(), internalCohort)) == CohortMetadata.Type.AGGREGATE_FAMILY) {
                    CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(study.getId(), internalCohort);
                    String status;
                    if (cohortMetadata.getAggregateFamilyStatus() == TaskMetadata.Status.READY) {
                        status = IndexStatus.READY;
                    } else {
                        status = IndexStatus.NONE;
                    }
                    Set<String> samples = new LinkedHashSet<>(cohortMetadata.getSamples().size());
                    for (Integer sampleId : cohortMetadata.getSamples()) {
                        samples.add(metadataManager.getSampleName(study.getId(), sampleId));
                    }
                    SampleInternalVariantAggregateFamily aggregateFamily = null;
                    for (SampleInternalVariantAggregateFamily thisAggregateFamily : aggregateFamilies) {
                        if (samples.size() == thisAggregateFamily.getSampleIds().size()
                                && samples.containsAll(thisAggregateFamily.getSampleIds())) {
                            aggregateFamily = thisAggregateFamily;
                            break;
                        }
                    }
                    if (aggregateFamily == null) {
                        aggregateFamily = new SampleInternalVariantAggregateFamily(new IndexStatus(status), new ArrayList<>(samples));
                        modified = true;
                        aggregateFamilyModified = true;
                        aggregateFamiliesUpdated.add(aggregateFamily);
                    } else {
                        if (aggregateFamily.getStatus().getId().equals(status)) {
                            // Same samples, same status. Nothing to update.
                            aggregateFamiliesUpdated.add(aggregateFamily);
                        } else {
                            modified = true;
                            aggregateFamilyModified = true;
                            aggregateFamily.setStatus(new IndexStatus(status));
                            aggregateFamiliesUpdated.add(aggregateFamily);
                        }
                    }
                }
            }
            if (aggregateFamiliesUpdated.size() != aggregateFamilies.size()) {
                // Deleted aggregate families. Force update
                modified = true;
                aggregateFamilyModified = true;
            }
            if (aggregateFamilyModified) {
                catalogManager.getSampleManager()
                        .updateSampleInternalVariantAggregateFamily(study.getName(), sample, aggregateFamiliesUpdated, token);
            }
        }

        return modified;
    }

    private static IndexStatus toIndexStatus(TaskMetadata.Status indexStatus) {
        return toIndexStatus(indexStatus, "");
    }

    private static IndexStatus toIndexStatus(TaskMetadata.Status storageStatus, String message) {
        String statusName;
        switch (storageStatus) {
            case NONE:
            case ERROR:
            case ABORTED:
                statusName = IndexStatus.NONE;
                break;
            case RUNNING:
                statusName = IndexStatus.INDEXING;
                break;
            case DONE:
            case READY:
                statusName = IndexStatus.READY;
                break;
            case INVALID:
                statusName = IndexStatus.INVALID;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + storageStatus);
        }
        if (message.isEmpty()) {
            if (statusName.equals(storageStatus.name())) {
                message = "Index is " + statusName.toLowerCase();
            } else {
                message = "Index is " + statusName.toLowerCase() + " (storage status: " + storageStatus.name().toLowerCase() + ")";
            }
        }
        return new IndexStatus(statusName, message);
    }

    public void synchronizeRemovedStudyFromStorage(String study, String token) throws CatalogException {
        catalogManager.getCohortManager().update(study, StudyEntry.DEFAULT_COHORT,
                new CohortUpdateParams().setSamples(Collections.emptyList()),
                true,
                new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(), "SET")),
                token);

        catalogManager.getCohortManager().setStatus(study, StudyEntry.DEFAULT_COHORT, CohortStatus.NONE,
                "Study has been removed from storage", token);

        VariantIndexStatus statusNone = new VariantIndexStatus(VariantIndexStatus.NONE, "Study has been removed from storage");
        try (DBIterator<File> iterator = catalogManager.getFileManager()
                .iterator(study, INDEXED_FILES_QUERY, INDEXED_FILES_QUERY_OPTIONS, token)) {
            while (iterator.hasNext()) {
                File file = iterator.next();
                catalogManager.getFileManager().updateFileInternalVariantIndex(study, file,
                        FileInternalVariantIndex.init().setStatus(statusNone), token);
                catalogManager.getFileManager().updateFileInternalVariantAnnotationIndex(study, file,
                        FileInternalVariantAnnotationIndex.init().setStatus(statusNone), token);
                catalogManager.getFileManager().updateFileInternalVariantSecondaryAnnotationIndex(study, file,
                        FileInternalVariantSecondaryAnnotationIndex.init().setStatus(statusNone), token);
            }
        }
        try (DBIterator<Sample> iterator = catalogManager.getSampleManager()
                .iterator(study, INDEXED_SAMPLES_QUERY, SAMPLE_QUERY_OPTIONS, token)) {
            while (iterator.hasNext()) {
                Sample sample = iterator.next();
                catalogManager.getSampleManager().updateSampleInternalVariantIndex(study, sample,
                        new SampleInternalVariantIndex().setStatus(statusNone), token);
                catalogManager.getSampleManager().updateSampleInternalVariantAnnotationIndex(study, sample,
                        new SampleInternalVariantAnnotationIndex().setStatus(statusNone), token);
                catalogManager.getSampleManager().updateSampleInternalVariantSecondaryAnnotationIndex(study, sample,
                        new SampleInternalVariantSecondaryAnnotationIndex().setStatus(statusNone), token);
                catalogManager.getSampleManager().updateSampleInternalVariantSecondarySampleIndex(study, sample,
                        new SampleInternalVariantSecondarySampleIndex().setStatus(statusNone).setVersion(-1), token);
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
