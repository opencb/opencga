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

package org.opencb.opencga.analysis.storage.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.storage.variant.VariantStorageManager;
import org.opencb.opencga.analysis.storage.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

/**
 * Created by jacobo on 06/03/15.
 */
@Analysis(id = VariantStatsAnalysis.ID, type = Analysis.AnalysisType.VARIANT)
public class VariantStatsStorageOperation extends OpenCgaAnalysis {

    private List<String> cohorts;
    private String studyFqn;

    private boolean overwriteStats;
    private boolean updateStats;
    private boolean resume;
    private URI outdirUri;
    private List<String> cohortIds;


    public VariantStatsStorageOperation setCohorts(List<String> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public VariantStatsStorageOperation setStudyId(String studyId) {
        this.studyFqn = studyId;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();

        overwriteStats = params.getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(), false);
        updateStats = params.getBoolean(VariantStorageOptions.STATS_UPDATE.key(), false);
        resume = params.getBoolean(VariantStorageOptions.RESUME.key(), VariantStorageOptions.RESUME.defaultValue());

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, userId);
        studyFqn = study.getFqn();


        // Do not save intermediate files
        outdirUri = getScratchDir().toUri();
    }

    @Override
    protected void run() throws Exception {

        Aggregation aggregation = getAggregation(catalogManager, studyFqn, params, sessionId);

        DataStore dataStore = VariantStorageManager.getDataStore(catalogManager, studyFqn, File.Bioformat.VARIANT, sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        CatalogStorageMetadataSynchronizer synchronizer = new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager());

        synchronizer.synchronizeCatalogStudyFromStorage(studyFqn, sessionId);

        cohortIds = checkCohorts(studyFqn, aggregation, cohorts, sessionId);
        Map<String, List<String>> cohortsMap = checkCanCalculateCohorts(studyFqn, cohortIds, updateStats, resume, sessionId);


        String outputFileName = buildOutputFileName(cohortIds, params.getString(VariantQueryParam.REGION.key()));

        QueryOptions calculateStatsOptions = new QueryOptions(params)
                .append(VariantStorageOptions.STATS_AGGREGATION.key(), aggregation)
                .append(VariantStorageOptions.STATS_OVERWRITE.key(), overwriteStats)
                .append(VariantStorageOptions.STATS_UPDATE.key(), updateStats)
                .append(VariantStorageOptions.RESUME.key(), resume);

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        String aggregationMappingFile = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());
        if (AggregationUtils.isAggregated(aggregation) && StringUtils.isNotEmpty(aggregationMappingFile)) {
            Properties mappingFile = readAggregationMappingFile(aggregationMappingFile);
            calculateStatsOptions.append(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), mappingFile);
        }

        // Up to this point, catalog has not been modified
        try {
            // Modify cohort status to "CALCULATING"
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.CALCULATING, "Start calculating stats");

            calculateStatsOptions.put(DefaultVariantStatisticsManager.OUTPUT, outdirUri.resolve(outputFileName));
            variantStorageEngine.getOptions().putAll(calculateStatsOptions);
            variantStorageEngine.calculateStats(studyFqn, cohortsMap, calculateStatsOptions);

            // Modify cohort status to "READY"
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.READY, "");

            variantStorageEngine.close();
        } catch (Exception e) {
            // Error!
            logger.error("Error executing stats. Set cohorts status to " + Cohort.CohortStatus.INVALID, e);
            // Modify to "INVALID"
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.INVALID, "Error calculating stats: " + e.getMessage());
            throw new StorageEngineException("Error calculating statistics.", e);
        }
    }

    @Override
    protected void onShutdown() {
        try {
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.INVALID, "");
        } catch (CatalogException e) {
            logger.error("Error updating cohorts " + cohortIds + " to status " + Cohort.CohortStatus.INVALID, e);
        }
    }

    private VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        return StorageEngineFactory.get(variantStorageManager.getStorageConfiguration())
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
    }

    protected String buildOutputFileName(List<String> cohortIds, String region) {
        final String outputFileName;
        if (isNotEmpty(params.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME))) {
            outputFileName = params.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME);
        } else {
            StringBuilder outputFileNameBuilder;
            outputFileNameBuilder = new StringBuilder("stats_");
            if (isNotEmpty(region)) {
                outputFileNameBuilder.append(region).append('_');
            }
            for (Iterator<String> iterator = cohortIds.iterator(); iterator.hasNext();) {
                String cohortId = iterator.next();
                outputFileNameBuilder.append(cohortId);
                if (iterator.hasNext()) {
                    outputFileNameBuilder.append('_');
                }
            }
            outputFileName = outputFileNameBuilder.toString();
        }
        return outputFileName;
    }

    /**
     * Must provide a list of cohorts or a aggregation_mapping_properties file.
     * @param studyId   Study
     * @param aggregation Aggregation type for this study. {@link #getAggregation}
     * @param cohorts   List of cohorts
     * @param sessionId User's sessionId
     * @return          Checked list of cohorts
     * @throws CatalogException if an error on Catalog
     * @throws IOException if an IO error reading the aggregation map file (if any)
     */
    protected List<String> checkCohorts(String studyId, Aggregation aggregation, List<String> cohorts, String sessionId)
            throws CatalogException, IOException {
        List<String> cohortIds;

        // Check aggregation mapping properties
        String tagMap = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());
        List<String> cohortsByAggregationMapFile = Collections.emptyList();
        if (!isBlank(tagMap)) {
            if (!AggregationUtils.isAggregated(aggregation)) {
                throw nonAggregatedWithMappingFile();
            }
            cohortsByAggregationMapFile = createCohortsByAggregationMapFile(studyId, tagMap, sessionId);
        } else if (AggregationUtils.isAggregated(aggregation)) {
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

    private List<String> createCohortsByAggregationMapFile(String studyId, String aggregationMapFile, String sessionId)
            throws IOException, CatalogException {
        Properties tagmap = readAggregationMappingFile(aggregationMapFile);
        Set<String> cohortNames = VariantAggregatedStatsCalculator.getCohorts(tagmap);
        return createCohortsIfNeeded(studyId, cohortNames, sessionId);
    }

    private Properties readAggregationMappingFile(String aggregationMapFile) throws IOException {
        Properties tagmap = new Properties();
        try (InputStream is = FileUtils.newInputStream(Paths.get(aggregationMapFile))) {
            tagmap.load(is);
        }
        return tagmap;
    }

    private List<String> createCohortsIfNeeded(String studyId, Set<String> cohortNames, String sessionId) throws CatalogException {
        List<String> cohorts = new ArrayList<>();
        // Silent query, so it does not fail for missing cohorts
        Set<String> catalogCohorts = catalogManager.getCohortManager().get(studyId, new ArrayList<>(cohortNames),
                new QueryOptions(QueryOptions.INCLUDE, "name,id"), true, sessionId)
                .getResults()
                .stream()
                .filter(Objects::nonNull)
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        for (String cohortName : cohortNames) {
            if (!catalogCohorts.contains(cohortName)) {
                DataResult<Cohort> cohort = catalogManager.getCohortManager().create(studyId, cohortName, Study.Type.COLLECTION, "",
                        Collections.emptyList(), null, null, sessionId);
                logger.info("Creating cohort {}", cohortName);
                cohorts.add(cohort.first().getId());
            } else {
                logger.debug("cohort {} was already created", cohortName);
                cohorts.add(cohortName);
            }
        }
        return cohorts;
    }

    /**
     * Check if a set of given cohorts are available to calculate statistics.
     *
     * @param studyFqn      Study fqn
     * @param cohortIds     Set of cohorts
     * @param updateStats   Update already existing stats
     * @param resume        Resume statistics calculation
     * @param sessionId     User's sessionId
     * @return Map from cohortId to Cohort
     * @throws CatalogException if an error on Catalog
     */
    protected Map<String, List<String>> checkCanCalculateCohorts(String studyFqn, List<String> cohortIds,
                                                         boolean updateStats, boolean resume, String sessionId)
            throws CatalogException {
//        Set<Long> studyIdSet = new HashSet<>();
        Map<String, List<String>> cohortMap = new HashMap<>(cohortIds.size());
        for (String cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohortManager().get(studyFqn, cohortId, null, sessionId)
                    .first();
//            long studyIdByCohortId = catalogManager.getCohortManager().getStudyId(cohortUid);
//            studyIdSet.add(studyIdByCohortId);
            switch (cohort.getStatus().getName()) {
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                case Cohort.CohortStatus.READY:
                    if (updateStats) {
                        catalogManager.getCohortManager().setStatus(studyFqn, cohortId, Cohort.CohortStatus.INVALID, "", sessionId);
                        break;
                    } else {
                        // If not updating the stats or resuming, can't calculate statistics for a cohort READY
                        if (!resume) {
                            throw unableToCalculateCohortReady(cohort);
                        }
                    }
                    break;
                case Cohort.CohortStatus.CALCULATING:
                    if (!resume) {
                        throw unableToCalculateCohortCalculating(cohort);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown status " + cohort.getStatus().getName());
            }
            cohortMap.put(cohort.getId(), cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
//            DataResult<Sample> sampleDataResult = catalogManager.getAllSamples(studyIdByCohortId, new Query("id", cohort.getSamples()),
//                      new QueryOptions(), sessionId);
        }

        // Check that all cohorts are from the same study
        // All cohorts are from the same study, as the study is a required parameter.
//        if (studyIdSet.size() != 1) {
//            throw new CatalogException("Error: CohortIds are from multiple studies: " + studyIdSet.toString());
//        }
//        if (!new ArrayList<>(studyIdSet).get(0).equals(studyFqn)) {
//            throw new CatalogException("Error: CohortIds are from a different study than provided: " + studyIdSet.toString());
//        }
        return cohortMap;
    }

    protected void updateCohorts(String studyId, List<String> cohortIds, String sessionId, String status, String message)
            throws CatalogException {
        for (String cohortId : cohortIds) {
            catalogManager.getCohortManager().setStatus(studyId, cohortId, status, message, sessionId);
        }
    }

    /**
     * If the study is aggregated and a mapping file is provided, pass it to
     * and create in catalog the cohorts described in the mapping file.
     *
     * If the study aggregation was not defined, updateStudy with the provided aggregation type
     *
     * @param catalogManager CatalogManager
     * @param studyId   StudyId where calculate stats
     * @param options   Options
     * @param sessionId Users sessionId
     * @return          Effective study aggregation type
     * @throws CatalogException if something is wrong with catalog
     */
    public static Aggregation getAggregation(CatalogManager catalogManager, String studyId, ObjectMap options, String sessionId)
            throws CatalogException {
        QueryOptions include = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.ATTRIBUTES.key());
        Study study = catalogManager.getStudyManager().get(studyId, include, sessionId).first();
        Aggregation argsAggregation = options.get(VariantStorageOptions.STATS_AGGREGATION.key(), Aggregation.class, Aggregation.NONE);
        Object studyAggregationObj = study.getAttributes().get(VariantStorageOptions.STATS_AGGREGATION.key());
        Aggregation studyAggregation = null;
        if (studyAggregationObj != null) {
            studyAggregation = AggregationUtils.valueOf(studyAggregationObj.toString());
        }

        final Aggregation aggregation;
        if (AggregationUtils.isAggregated(argsAggregation)) {
            if (studyAggregation != null && !studyAggregation.equals(argsAggregation)) {
                // FIXME: Throw an exception?
                LoggerFactory.getLogger(StorageOperation.class)
                        .warn("Calculating statistics with aggregation " + argsAggregation + " instead of " + studyAggregation);
            }
            aggregation = argsAggregation;
            // If studyAggregation is not define, update study aggregation
            if (studyAggregation == null) {
                //update study aggregation
                Map<String, Aggregation> attributes = Collections.singletonMap(VariantStorageOptions.STATS_AGGREGATION.key(),
                        argsAggregation);
                ObjectMap parameters = new ObjectMap("attributes", attributes);
                catalogManager.getStudyManager().update(studyId, parameters, null, sessionId);
            }
        } else {
            if (studyAggregation == null) {
                aggregation = Aggregation.NONE;
            } else {
                aggregation = studyAggregation;
            }
        }
        return aggregation;
    }

    static CatalogException differentCohortsThanMappingFile() throws CatalogException {
        return new CatalogException("Given cohorts (if any) must match with cohorts in the aggregation mapping file.");
    }

    static CatalogException missingCohorts() throws CatalogException {
        return new CatalogException("Cohort list null or empty");
    }

    static IllegalArgumentException missingAggregationMappingFile(Aggregation aggregation) {
        return new IllegalArgumentException("Unable to calculate statistics for an aggregated study of type "
                + "\"" + aggregation + "\" without an aggregation mapping file.");
    }

    static IllegalArgumentException nonAggregatedWithMappingFile() {
        return new IllegalArgumentException("Unable to use an aggregation mapping file for non aggregated study");
    }


    static CatalogException unableToCalculateCohortReady(Cohort cohort) {
        return new CatalogException("Unable to calculate stats for cohort "
                + "{ uid: " + cohort.getUid() + " id: \"" + cohort.getId() + "\" }"
                + " with status \"" + cohort.getStatus().getName() + "\". "
                + "Resume or update stats for continue calculation");
    }

    static CatalogException unableToCalculateCohortCalculating(Cohort cohort) {
        return new CatalogException("Unable to calculate stats for cohort "
                + "{ uid: " + cohort.getUid() + " id: \"" + cohort.getId() + "\" }"
                + " with status \"" + cohort.getStatus().getName() + "\". "
                + "Resume for continue calculation.");
    }

}
