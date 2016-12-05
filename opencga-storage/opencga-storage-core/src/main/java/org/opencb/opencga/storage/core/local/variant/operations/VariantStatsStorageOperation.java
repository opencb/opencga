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

package org.opencb.opencga.storage.core.local.variant.operations;

import org.opencb.biodata.models.variant.VariantSource.Aggregation;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
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
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;

/**
 * Created by jacobo on 06/03/15.
 */
public class VariantStatsStorageOperation extends StorageOperation {

    public VariantStatsStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageManagerFactory.get(storageConfiguration), LoggerFactory.getLogger(VariantStatsStorageOperation.class));
    }

    public void calculateStats(long studyId, List<String> cohorts, String outdirStr, String catalogOutDirIdStr,
                               QueryOptions options, String sessionId)
            throws CatalogException, IOException, URISyntaxException, StorageManagerException {
        Job.Type step = Job.Type.COHORT_STATS;
        String fileIdStr = options.getString(Options.FILE_ID.key(), null);
        boolean overwriteStats = options.getBoolean(Options.OVERWRITE_STATS.key(), false);
        boolean updateStats = options.getBoolean(Options.UPDATE_STATS.key(), false);
        final Long fileId = fileIdStr == null ? null : catalogManager.getFileId(fileIdStr, sessionId);


        // Outdir must be empty
        URI outdirUri = UriUtils.createDirectoryUri(outdirStr);
        final Path outdir = Paths.get(outdirUri);
        outdirMustBeEmpty(outdir);

        List<Long> cohortIds = checkCohorts(studyId, cohorts, options, sessionId);
        Map<Long, Cohort> cohortsMap = checkCanCalculateCohorts(studyId, cohortIds, updateStats, sessionId);


        String region = options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key());
        String outputFileName = buildOutputFileName(cohortIds, options, cohortsMap, region);

        Long catalogOutDirId = getCatalogOutdirId(studyId, catalogOutDirIdStr, sessionId);

        QueryOptions calculateStatsOptions = new QueryOptions(options)
//                .append(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100)
//                .append(VariantStorageManager.Options.LOAD_THREADS.key(), 6)
                .append(Options.OVERWRITE_STATS.key(), overwriteStats)
                .append(Options.UPDATE_STATS.key(), updateStats);
        calculateStatsOptions.putIfNotNull(Options.FILE_ID.key(), fileId);
        calculateStatsOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REGION.key(), region);

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        Aggregation aggregation = getAggregation(studyId, options, sessionId);
        String aggregationMappingFile = options.getString(Options.AGGREGATION_MAPPING_PROPERTIES.key());
        if (Aggregation.isAggregated(aggregation) && !aggregationMappingFile.isEmpty()) {
            try (InputStream is = FileUtils.newInputStream(Paths.get(aggregationMappingFile))) {
                Properties properties = new Properties();
                properties.load(is);
                calculateStatsOptions.append(Options.AGGREGATION_MAPPING_PROPERTIES.key(), properties);
            }
        }

        DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyId, dataStore);

        Thread hook = buildHook(cohortIds, sessionId, outdir);
        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
        Runtime.getRuntime().addShutdownHook(hook);
        // Up to this point, catalog has not been modified
        try {
            // Modify cohort status to "CALCULATING"
            updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.CALCULATING, "Start calculating stats");

            calculateStatsOptions.put(DefaultVariantStatisticsManager.OUTPUT, outdirUri.resolve(outputFileName));
            VariantStorageManager variantStorageManager
                    = storageManagerFactory.getVariantStorageManager(dataStore.getStorageEngine());
            List<String> cohortsName = cohortsMap.values().stream().map(Cohort::getName).collect(Collectors.toList());
            variantStorageManager.calculateStats(studyConfiguration.getStudyName(), cohortsName, dataStore.getDbName(),
                    calculateStatsOptions);

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
                copyResults(Paths.get(outdirUri), catalogOutDirId, sessionId);
            }

            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
            // Modify cohort status to "READY"
            updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.READY, "");
        } catch (Exception e) {
            // Error!
            logger.error("Error executing stats. Set cohorts status to " + Cohort.CohortStatus.INVALID, e);
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            // Modify to "INVALID"
            updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.INVALID, "Error calculating stats: " + e.getMessage());
            throw new StorageManagerException("Error calculating statistics.", e);
        } finally {
            // Remove hook
            Runtime.getRuntime().removeShutdownHook(hook);
        }

    }


    protected Thread buildHook(List<Long> cohortIds, String sessionId, Path outdir) {
        return buildHook(outdir, () -> {
            try {
                updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.INVALID, "");
            } catch (CatalogException e) {
                logger.error("Error updating cohorts " + cohortIds + " to status " + Cohort.CohortStatus.INVALID, e);
            }
        });
    }

    protected String buildOutputFileName(List<Long> cohortIds, QueryOptions options, Map<Long, Cohort> cohortsMap, String region) {
        final String outputFileName;
        if (isNotEmpty(options.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME))) {
            outputFileName = options.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME);
        } else {
            StringBuilder outputFileNameBuilder;
            outputFileNameBuilder = new StringBuilder("stats_");
            if (isNotEmpty(region)) {
                outputFileNameBuilder.append(region).append("_");
            }
            for (Iterator<Long> iterator = cohortIds.iterator(); iterator.hasNext();) {
                Long cohortId = iterator.next();
                outputFileNameBuilder.append(cohortsMap.get(cohortId).getName());
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
     * @param studyId   StudyId
     * @param cohorts   List of cohorts
     * @param options   Options, where the aggregation mapping properties file will be
     * @param sessionId User's sessionId
     * @return          Checked list of cohorts
     * @throws CatalogException if an error on Catalog
     * @throws IOException if an IO error reading the aggregation map file (if any)
     */
    protected List<Long> checkCohorts(long studyId, List<String> cohorts, QueryOptions options, String sessionId)
            throws CatalogException, IOException {
        List<Long> cohortIds;

        // Check aggregation mapping properties
        String tagMap = options.getString(Options.AGGREGATION_MAPPING_PROPERTIES.key());
        List<Long> cohortsByAggregationMapFile = null;
        if (!isBlank(tagMap)) {
            cohortsByAggregationMapFile = createCohortsByAggregationMapFile(studyId, tagMap, sessionId);
        }

        if (cohorts == null || cohorts.isEmpty()) {
            // If no aggregation map file provided
            if (cohortsByAggregationMapFile == null || cohortsByAggregationMapFile.isEmpty()) {
                throw new CatalogException("Cohort list null or empty");
            } else {
                cohortIds = cohortsByAggregationMapFile;
            }
        } else {
            cohortIds = new ArrayList<>(cohorts.size());
            for (String cohort : cohorts) {
                if (!cohort.contains(":")) {
                    cohort = studyId + ":" + cohort;
                }
                long cohortId = catalogManager.getCohortId(cohort, sessionId);
                if (cohortId < 0) {
                    throw new CatalogException("Cohort '" + cohort + "' not found");
                }
                cohortIds.add(cohortId);
            }
            if (cohortsByAggregationMapFile != null) {
                if (cohortIds.size() != cohortsByAggregationMapFile.size() || !cohortIds.containsAll(cohortsByAggregationMapFile)) {
                    throw new CatalogException("Given cohorts (if any) must match with cohorts in the aggregation mapping file.");
                }
            }
        }
        return cohortIds;
    }

    private List<Long> createCohortsByAggregationMapFile(long studyId, String aggregationMapFile, String sessionId)
            throws IOException, CatalogException {
        List<Long> cohorts = new ArrayList<>();
        Properties tagmap = new Properties();
        try (InputStream is = new FileInputStream(aggregationMapFile)) {
            tagmap.load(is);
        }
        Map<String, Long> catalogCohorts = catalogManager.getAllCohorts(studyId, null,
                new QueryOptions(QueryOptions.INCLUDE, "name,id"), sessionId).getResult()
                .stream()
                .collect(Collectors.toMap(Cohort::getName, Cohort::getId));
        for (String cohortName : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
            if (!catalogCohorts.containsKey(cohortName)) {
                QueryResult<Cohort> cohort = catalogManager
                        .createCohort(studyId, cohortName, Study.Type.COLLECTION, "", Collections.emptyList(), null, sessionId);
                logger.info("Creating cohort {}", cohortName);
                cohorts.add(cohort.first().getId());
            } else {
                logger.debug("cohort {} was already created", cohortName);
                cohorts.add(catalogCohorts.get(cohortName));
            }
        }
        return cohorts;
    }

    /**
     * If the study is aggregated and a mapping file is provided, pass it to
     * and create in catalog the cohorts described in the mapping file.
     *
     * If the study aggregation was not defined, updateStudy with the provided aggregation type
     *
     * @param studyId   StudyId where calculate stats
     * @param options   Options
     * @param sessionId Users sessionId
     * @return          Effective study aggregation type
     * @throws CatalogException if something is wrong with catalog
     */
    public Aggregation getAggregation(long studyId, QueryOptions options, String sessionId) throws CatalogException {
        Study study = catalogManager.getStudy(studyId, new QueryOptions("include", "projects.studies.attributes"), sessionId).first();
        Aggregation argsAggregation = options.get(Options.AGGREGATED_TYPE.key(), Aggregation.class, Aggregation.NONE);
        Object studyAggregationObj = study.getAttributes().get(Options.AGGREGATED_TYPE.key());
        Aggregation studyAggregation = null;
        if (studyAggregationObj != null) {
            studyAggregation = Aggregation.valueOf(studyAggregationObj.toString());
        }

        final Aggregation aggregation;
        if (Aggregation.isAggregated(argsAggregation)) {
            if (studyAggregation != null && !studyAggregation.equals(argsAggregation)) {
                // FIXME: Throw an exception?
                logger.warn("Calculating statistics with aggregation " + argsAggregation + " instead of " + studyAggregation);
            }
            aggregation = argsAggregation;
            // If studyAggregation is not define, update study aggregation
            if (studyAggregation == null) {
                //update study aggregation
                Map<String, Aggregation> attributes = Collections.singletonMap(Options.AGGREGATED_TYPE.key(), argsAggregation);
                ObjectMap parameters = new ObjectMap("attributes", attributes);
                catalogManager.modifyStudy(studyId, parameters, sessionId);
            }
        } else {
            aggregation = studyAggregation;
        }
        return aggregation;
    }

    /**
     * Check if a set of given cohorts are available to calculate statistics.
     *
     * @param studyId       Study id
     * @param cohortIds     Set of cohorts
     * @param updateStats   Update already existing stats
     * @param sessionId     User's sessionId
     * @return Map from cohortId to Cohort
     * @throws CatalogException if an error on Catalog
     */
    protected Map<Long, Cohort> checkCanCalculateCohorts(long studyId, List<Long> cohortIds, boolean updateStats, String sessionId)
            throws CatalogException {
        Set<Long> studyIdSet = new HashSet<>();
        Map<Long, Cohort> cohortMap = new HashMap<>(cohortIds.size());
        for (Long cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohort(cohortId, null, sessionId).first();
            long studyIdByCohortId = catalogManager.getStudyIdByCohortId(cohortId);
            studyIdSet.add(studyIdByCohortId);
            switch (cohort.getStatus().getName()) {
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                case Cohort.CohortStatus.READY:
                    if (updateStats) {
                        catalogManager.getCohortManager().setStatus(cohortId.toString(), Cohort.CohortStatus.INVALID, "", sessionId);
                        break;
                    }
                case Cohort.CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to calculate stats for cohort "
                            + "{ id: " + cohort.getId() + " name: \"" + cohort.getName() + "\" }"
                            + " with status \"" + cohort.getStatus().getName() + "\"");
                default:
                    throw new IllegalStateException("Unknown status " + cohort.getStatus().getName());
            }
            cohortMap.put(cohortId, cohort);
//            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyIdByCohortId, new Query("id", cohort.getSamples()),
//                      new QueryOptions(), sessionId);
        }

        // Check that all cohorts are from the same study
        if (studyIdSet.size() != 1) {
            throw new CatalogException("Error: CohortIds are from multiple studies: " + studyIdSet.toString());
        }
        if (!new ArrayList<>(studyIdSet).get(0).equals(studyId)) {
            throw new CatalogException("Error: CohortIds are from a different study than provided: " + studyIdSet.toString());
        }

        return cohortMap;
    }

    protected void updateCohorts(List<Long> cohortIds, String sessionId, String status, String message) throws CatalogException {
        for (Long cohortId : cohortIds) {
            catalogManager.getCohortManager().setStatus(cohortId.toString(), status, message, sessionId);
        }
    }


}
