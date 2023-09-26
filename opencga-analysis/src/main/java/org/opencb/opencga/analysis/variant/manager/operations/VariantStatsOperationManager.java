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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis.getAggregation;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.REGION;

public class VariantStatsOperationManager extends OperationManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public VariantStatsOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine variantStorageEngine) {
        super(variantStorageManager, variantStorageEngine);
    }


    public Collection<String> stats(String study, List<String> cohorts, String region, ObjectMap params, String token)
            throws IOException, StorageEngineException, CatalogException {
        Aggregation aggregation = getAggregation(catalogManager, study, params, token);
        cohorts = checkCohorts(study, aggregation, cohorts, params, token);

        boolean overwriteStats = params.getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(), false);
        boolean resume = params.getBoolean(VariantStorageOptions.RESUME.key(), VariantStorageOptions.RESUME.defaultValue());

        // Synchronize catalog with storage
        CatalogStorageMetadataSynchronizer synchronizer =
                new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager());
        synchronizer.synchronizeCatalogStudyFromStorage(study, token);

        Map<String, List<String>> cohortsMap = checkCanCalculateCohorts(study, cohorts, overwriteStats, resume, token);

        QueryOptions calculateStatsOptions = new QueryOptions(params);
        calculateStatsOptions.putIfNotEmpty(REGION.key(), region);


        try {
            // Modify cohort status to "CALCULATING"
            updateCohorts(study, cohortsMap.keySet(), token, CohortStatus.CALCULATING, "Start calculating stats");

            variantStorageEngine.calculateStats(study, cohortsMap, calculateStatsOptions);

            // Modify cohort status to "READY"
            updateCohorts(study, cohortsMap.keySet(), token, CohortStatus.READY, "");
        } catch (Exception e) {
            // Error!
            logger.error("Error executing stats. Set cohorts status to " + CohortStatus.INVALID, e);
            // Modify to "INVALID"
            try {
                updateCohorts(study, cohortsMap.keySet(), token, CohortStatus.INVALID,
                        "Error calculating stats: " + e.getMessage());
            } catch (CatalogException ex) {
                e.addSuppressed(ex);
            }
            throw new StorageEngineException("Error calculating statistics.", e);
        }
        return cohortsMap.keySet();

    }

    public Collection<String> delete(String study, List<String> cohorts, ObjectMap params, String token)
            throws CatalogException, StorageEngineException {

        // Synchronize catalog with storage
        CatalogStorageMetadataSynchronizer synchronizer =
                new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager());
        synchronizer.synchronizeCatalogStudyFromStorage(study, token);

        try {
            // Modify cohort status to "INVALID"
            updateCohorts(study, cohorts, token, CohortStatus.INVALID, "Variant stats being deleted");

            variantStorageEngine.deleteStats(study, cohorts, params);

            // Modify cohort status to "NONE"
            updateCohorts(study, cohorts, token, CohortStatus.NONE, "");
        } catch (Exception e) {
            throw new StorageEngineException("Error calculating statistics.", e);
        }


        return cohorts;
    }

    protected void updateCohorts(String studyId, Collection<String> cohortIds, String sessionId, String status, String message)
            throws CatalogException {
        for (String cohortId : cohortIds) {
            catalogManager.getCohortManager().setStatus(studyId, cohortId, status, message, sessionId);
        }
    }

    /**
     * Must provide a list of cohorts or a aggregation_mapping_properties file.
     * @param studyId   Study
     * @param aggregation Aggregation type for this study. {@link org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis#getAggregation}
     * @param cohorts   List of cohorts
     * @param sessionId User's sessionId
     * @return          Checked list of cohorts
     * @throws CatalogException if an error on Catalog
     * @throws IOException if an IO error reading the aggregation map file (if any)
     */
    protected List<String> checkCohorts(String studyId, Aggregation aggregation, List<String> cohorts, ObjectMap params, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        List<String> cohortIds;

        // Check aggregation mapping properties
        String tagMap = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());

        List<String> cohortsByAggregationMapFile = Collections.emptyList();
        if (StringUtils.isNotEmpty(tagMap)) {
            logger.info("Aggregation : " + aggregation);
            logger.info("Aggregation tagMap : " + tagMap);
            if (!AggregationUtils.isAggregated(aggregation)) {
                throw nonAggregatedWithMappingFile();
            }
            cohortsByAggregationMapFile = createCohortsByAggregationMapFile(studyId, tagMap, sessionId);
            logger.info("Reading cohorts from aggregation mapping file. Found {} cohorts.",
                    cohortsByAggregationMapFile.size());
        } else if (AggregationUtils.isAggregated(aggregation)) {
//            throw missingAggregationMappingFile(aggregation);
            if (aggregation.equals(Aggregation.BASIC)) {
                cohortsByAggregationMapFile = createCohortsIfNeeded(studyId, Collections.singleton(StudyEntry.DEFAULT_COHORT), sessionId);
            } else {
                throw missingAggregationMappingFile(aggregation);
            }
        }

        if (cohorts == null || cohorts.isEmpty()) {
            // If no aggregation map file provided
            if (cohortsByAggregationMapFile.isEmpty()) {
                throw missingCohorts();
            } else {
                cohortIds = cohortsByAggregationMapFile;
            }
        } else {
            cohortIds = new ArrayList<>(cohorts.size());
            for (String cohort : cohorts) {
                String cohortId = catalogManager.getCohortManager().get(studyId, cohort,
                        new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key()), sessionId).first().getId();
                cohortIds.add(cohortId);
            }
            if (!cohortsByAggregationMapFile.isEmpty()) {
                if (cohortIds.size() != cohortsByAggregationMapFile.size() || !cohortIds.containsAll(cohortsByAggregationMapFile)) {
                    throw differentCohortsThanMappingFile();
                }
            }
        }
        return cohortIds;
    }

    /**
     * Check if a set of given cohorts are available to calculate statistics.
     *
     * @param studyFqn      Study fqn
     * @param cohortIds     Set of cohorts
     * @param overwriteStats Overwrite stats
     * @param resume        Resume statistics calculation
     * @param sessionId     User's sessionId
     * @return Map from cohortId to Cohort
     * @throws CatalogException if an error on Catalog
     */
    protected Map<String, List<String>> checkCanCalculateCohorts(String studyFqn, List<String> cohortIds,
                                                                 boolean overwriteStats, boolean resume, String sessionId)
            throws CatalogException, StorageEngineException {
        Map<String, List<String>> cohortMap = new HashMap<>(cohortIds.size());
        for (String cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohortManager()
                    .get(studyFqn, cohortId, CatalogStorageMetadataSynchronizer.COHORT_QUERY_OPTIONS, sessionId).first();
            switch (cohort.getInternal().getStatus().getId()) {
                case CohortStatus.NONE:
                case CohortStatus.INVALID:
                    break;
                case CohortStatus.READY:
                    if (overwriteStats) {
                        catalogManager.getCohortManager().setStatus(studyFqn, cohortId, CohortStatus.INVALID, "", sessionId);
                        break;
                    } else {
                        // If not updating the stats or resuming, can't calculate statistics for a cohort READY
                        if (!resume) {
                            throw unableToCalculateCohortReady(cohort);
                        }
                    }
                    break;
                case CohortStatus.CALCULATING:
                    if (!resume) {
                        throw unableToCalculateCohortCalculating(cohort);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown status " + cohort.getInternal().getStatus().getId());
            }
            cohortMap.put(cohort.getId(), cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
        }
        return cohortMap;
    }

    private List<String> createCohortsByAggregationMapFile(String studyId, String aggregationMapFile, String sessionId)
            throws IOException, CatalogException {
        Properties tagmap = readAggregationMappingFile(aggregationMapFile);
        Set<String> cohortNames = VariantAggregatedStatsCalculator.getCohorts(tagmap);
        return createCohortsIfNeeded(studyId, cohortNames, sessionId);
    }

    private Properties readAggregationMappingFile(String aggregationMapFile) throws IOException {
        try (InputStream is = FileUtils.newInputStream(Paths.get(aggregationMapFile))) {
            Properties tagmap = new Properties();
            tagmap.load(is);
            return tagmap;
        }
    }

    private List<String> createCohortsIfNeeded(String studyId, Set<String> cohortNames, String sessionId) throws CatalogException {
        List<String> cohorts = new ArrayList<>();
        // Silent query, so it does not fail for missing cohorts
        Set<String> catalogCohorts = catalogManager.getCohortManager().get(studyId, new ArrayList<>(cohortNames),
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        CohortDBAdaptor.QueryParams.ID.key(),
                        CohortDBAdaptor.QueryParams.UID.key(),
                        CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key()
                )), true, sessionId)
                .getResults()
                .stream()
                .filter(Objects::nonNull)
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        for (String cohortName : cohortNames) {
            if (!catalogCohorts.contains(cohortName)) {
                DataResult<Cohort> cohort = catalogManager.getCohortManager().create(studyId, new CohortCreateParams(cohortName,
                                "", Enums.CohortType.COLLECTION, "", null, null, Collections.emptyList(), null, null, null), null, null,
                        new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), sessionId);
                logger.info("Creating cohort {}", cohortName);
                cohorts.add(cohort.first().getId());
            } else {
                logger.debug("cohort {} was already created", cohortName);
                cohorts.add(cohortName);
            }
        }
        return cohorts;
    }


    public static StorageEngineException differentCohortsThanMappingFile() {
        return new StorageEngineException("Given cohorts (if any) must match with cohorts in the aggregation mapping file.");
    }

    public static StorageEngineException missingCohorts() {
        return new StorageEngineException("Unable to index stats if no cohort is specified.");
    }

    public static IllegalArgumentException missingAggregationMappingFile(Aggregation aggregation) {
        return new IllegalArgumentException("Unable to calculate statistics for an aggregated study of type "
                + "\"" + aggregation + "\" without an aggregation mapping file.");
    }

    public static IllegalArgumentException nonAggregatedWithMappingFile() {
        return new IllegalArgumentException("Unable to use an aggregation mapping file for non aggregated study");
    }


    public static StorageEngineException unableToCalculateCohortReady(Cohort cohort) {
        return new StorageEngineException("Unable to calculate stats for cohort "
                + "{ uid: " + cohort.getUid() + " id: \"" + cohort.getId() + "\" }"
                + " with status \"" + cohort.getInternal().getStatus().getId() + "\". "
                + "Resume or overwrite stats for continue calculation");
    }

    public static StorageEngineException unableToCalculateCohortCalculating(Cohort cohort) {
        return new StorageEngineException("Unable to calculate stats for cohort "
                + "{ uid: " + cohort.getUid() + " id: \"" + cohort.getId() + "\" }"
                + " with status \"" + cohort.getInternal().getStatus().getId() + "\". "
                + "Resume for continue calculation.");
    }
}
