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

package org.opencb.opencga.storage.core.manager.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;

/**
 * Created by jacobo on 06/03/15.
 */
public class VariantStatsStorageOperation extends StorageOperation {

    public VariantStatsStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageEngineFactory.get(storageConfiguration), LoggerFactory.getLogger(VariantStatsStorageOperation.class));
    }

    public void calculateStats(String studyStr, List<String> cohorts, String outdirStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException, URISyntaxException, StorageEngineException {
        boolean overwriteStats = options.getBoolean(Options.OVERWRITE_STATS.key(), false);
        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
        boolean resume = options.getBoolean(Options.RESUME.key(), Options.RESUME.defaultValue());

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        String studyFqn = study.getFqn();


        // Outdir must be empty
        URI outdirUri = UriUtils.createDirectoryUri(outdirStr);
        final Path outdir = Paths.get(outdirUri);
        outdirMustBeEmpty(outdir, options);

        Aggregation aggregation = getAggregation(studyFqn, options, sessionId);

        DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyFqn, File.Bioformat.VARIANT, sessionId);
        StudyConfiguration studyConfiguration = updateCatalogFromStudyConfiguration(sessionId, studyFqn, dataStore);

        List<String> cohortIds = checkCohorts(study, aggregation, cohorts, options, sessionId);
        Map<String, List<String>> cohortsMap = checkCanCalculateCohorts(studyFqn, cohortIds, updateStats, resume, sessionId);


        String region = options.getString(VariantQueryParam.REGION.key());
        String outputFileName = buildOutputFileName(cohortIds, options, region);

        String catalogOutDirId = getCatalogOutdirId(studyFqn, options, sessionId);

        QueryOptions calculateStatsOptions = new QueryOptions(options)
//                .append(VariantStorageEngine.Options.LOAD_BATCH_SIZE.key(), 100)
//                .append(VariantStorageEngine.Options.LOAD_THREADS.key(), 6)
                .append(Options.AGGREGATED_TYPE.key(), aggregation)
                .append(Options.OVERWRITE_STATS.key(), overwriteStats)
                .append(Options.UPDATE_STATS.key(), updateStats)
                .append(Options.RESUME.key(), resume);
        calculateStatsOptions.putIfNotEmpty(VariantQueryParam.REGION.key(), region);

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        String aggregationMappingFile = options.getString(Options.AGGREGATION_MAPPING_PROPERTIES.key());
        if (AggregationUtils.isAggregated(aggregation) && StringUtils.isNotEmpty(aggregationMappingFile)) {
            Properties mappingFile = readAggregationMappingFile(aggregationMappingFile);
            calculateStatsOptions.append(Options.AGGREGATION_MAPPING_PROPERTIES.key(), mappingFile);
        }


        Thread hook = buildHook(studyFqn, cohortIds, sessionId, outdir);
        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
        Runtime.getRuntime().addShutdownHook(hook);
        // Up to this point, catalog has not been modified
        try {
            // Modify cohort status to "CALCULATING"
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.CALCULATING, "Start calculating stats");

            calculateStatsOptions.put(DefaultVariantStatisticsManager.OUTPUT, outdirUri.resolve(outputFileName));
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
            variantStorageEngine.getOptions().putAll(calculateStatsOptions);
            variantStorageEngine.calculateStats(studyConfiguration.getStudyName(), cohortsMap, calculateStatsOptions);

//            DefaultVariantStatisticsManager variantStatisticsManager = new DefaultVariantStatisticsManager(dbAdaptor);
//
//            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(dataStore.getDbName());
//            Map<String, Integer> cohortNameIdMap = new HashMap<>(cohortIds.size());
//            Map<String, Set<String>> cohortSamplesMap = new HashMap<>(cohortIds.size());
//            for (Map.Entry<Long, Cohort> entry : cohortsMap.entrySet()) {
//                cohortNameIdMap.put(entry.getValue().getName(), entry.getKey().intValue());
//                cohortSamplesMap.put(entry.getValue().getName(), entry.getValue().getSamples()
//                        .stream()
//                        .map(sampleId -> {
//                            return studyConfiguration.getSampleIds().inverse().get(sampleId.intValue());
//                        })
//                        .collect(Collectors.toSet()));
//            }
//            URI stats = variantStatisticsManager.createStats(dbAdaptor, outdirUri.resolve(outputFileName), cohortSamplesMap,
//                    cohortNameIdMap, studyConfiguration, calculateStatsOptions);
//
//            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job still running. Statistics created."));
//            variantStatisticsManager.loadStats(dbAdaptor, stats, studyConfiguration, options);

            if (catalogOutDirId != null) {
                copyResults(Paths.get(outdirUri), studyStr, catalogOutDirId, sessionId);
            }

            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
            // Modify cohort status to "READY"
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.READY, "");
        } catch (Exception e) {
            // Error!
            logger.error("Error executing stats. Set cohorts status to " + Cohort.CohortStatus.INVALID, e);
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            // Modify to "INVALID"
            updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.INVALID, "Error calculating stats: " + e.getMessage());
            throw new StorageEngineException("Error calculating statistics.", e);
        } finally {
            // Remove hook
            Runtime.getRuntime().removeShutdownHook(hook);
        }

    }


    protected Thread buildHook(String studyFqn, List<String> cohortIds, String sessionId, Path outdir) {
        return buildHook(outdir, () -> {
            try {
                updateCohorts(studyFqn, cohortIds, sessionId, Cohort.CohortStatus.INVALID, "");
            } catch (CatalogException e) {
                logger.error("Error updating cohorts " + cohortIds + " to status " + Cohort.CohortStatus.INVALID, e);
            }
        });
    }

    protected String buildOutputFileName(List<String> cohortIds, QueryOptions options, String region) {
        final String outputFileName;
        if (isNotEmpty(options.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME))) {
            outputFileName = options.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME);
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
     * @param study   Study
     * @param aggregation Aggregation type for this study. {@link #getAggregation}
     * @param cohorts   List of cohorts
     * @param options   Options, where the aggregation mapping properties file will be
     * @param sessionId User's sessionId
     * @return          Checked list of cohorts
     * @throws CatalogException if an error on Catalog
     * @throws IOException if an IO error reading the aggregation map file (if any)
     */
    protected List<String> checkCohorts(Study study, Aggregation aggregation, List<String> cohorts, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        List<String> cohortIds;
        String studyId = study.getId();

        // Check aggregation mapping properties
        String tagMap = options.getString(Options.AGGREGATION_MAPPING_PROPERTIES.key());
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
                String cohortId = catalogManager.getCohortManager().get(study.getFqn(), cohort,
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
        Set<String> catalogCohorts = catalogManager.getCohortManager().get(studyId, new ArrayList<>(cohortNames), new Query(),
                new QueryOptions(QueryOptions.INCLUDE, "name,id"), true, sessionId)
                .stream()
                .map(QueryResult::first)
                .filter(Objects::nonNull)
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        for (String cohortName : cohortNames) {
            if (!catalogCohorts.contains(cohortName)) {
                QueryResult<Cohort> cohort = catalogManager.getCohortManager().create(studyId, cohortName, Study.Type.COLLECTION, "",
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
//            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyIdByCohortId, new Query("id", cohort.getSamples()),
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
