/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.analysis.storage.variant;

import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.JobFactory;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.analysis.variant.CatalogStudyConfigurationFactory;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.monitor.executors.AbstractExecutor;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Created by jacobo on 06/03/15.
 */
public class VariantStorage extends AbstractFileIndexer {

    public VariantStorage(CatalogManager catalogManager) {
        super(catalogManager, LoggerFactory.getLogger(VariantStorage.class));
    }

    public void calculateStats(List<Long> cohortIds, String catalogOutDirIdStr, String outdirStr, String sessionId, QueryOptions options)
            throws AnalysisExecutionException, CatalogException, IOException, URISyntaxException {
        Job.Type step = Job.Type.COHORT_STATS;
        String fileIdStr = options.getString(VariantStorageManager.Options.FILE_ID.key(), null);
        boolean overwriteStats = options.getBoolean(VariantStorageManager.Options.OVERWRITE_STATS.key(), false);
        boolean updateStats = options.getBoolean(VariantStorageManager.Options.UPDATE_STATS.key(), false);
        final Long fileId = fileIdStr == null ? null : catalogManager.getFileId(fileIdStr, sessionId);


        // Outdir must be empty
        URI outdirUri = UriUtils.createDirectoryUri(outdirStr);
        final Path outdir = Paths.get(outdirUri);
        outdirMustBeEmpty(outdir);

        cohortIds = checkAggregated(cohortIds, options);
        Map<Long, Cohort> cohortsMap = checkCanCalculateCohorts(cohortIds, updateStats, sessionId);
        long studyId = catalogManager.getStudyIdByCohortId(cohortIds.get(0));


        String region = options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key());
        String outputFileName = buildOutputFileName(cohortIds, options, cohortsMap, region);

        Long catalogOutDirId;
        if (catalogOutDirIdStr != null) {
            catalogOutDirId = catalogManager.getFileManager().getId(catalogOutDirIdStr, studyId, sessionId);
            if (catalogOutDirId <= 0) {
                throw new CatalogException("Output directory " + catalogOutDirIdStr + " could not be found within catalog.");
            }
        } else {
            catalogOutDirId = null;
        }

        QueryOptions calculateStatsOptions = new QueryOptions(options)
//                .append(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 100)
//                .append(VariantStorageManager.Options.LOAD_THREADS.key(), 6)
                .append(VariantStorageManager.Options.OVERWRITE_STATS.key(), overwriteStats)
                .append(VariantStorageManager.Options.UPDATE_STATS.key(), updateStats);
        calculateStatsOptions.putIfNotNull(VariantStorageManager.Options.FILE_ID.key(), fileId);
        calculateStatsOptions.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REGION.key(), region);

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        Study study = catalogManager.getStudy(studyId, new QueryOptions("include", "projects.studies.attributes"), sessionId).first();
        VariantSource.Aggregation studyAggregation = VariantSource.Aggregation.valueOf(study.getAttributes()
                .getOrDefault(VariantStorageManager.Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.NONE).toString());
        String aggregationMappingFile = options.getString(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key());
        if (VariantSource.Aggregation.isAggregated(studyAggregation)
                && !aggregationMappingFile.isEmpty()) {
            calculateStatsOptions
                    .append(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingFile);
        }

        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        StudyConfiguration studyConfiguration = updateStudyConfiguration(sessionId, studyId, dataStore);

        Thread hook = buildHook(cohortIds, sessionId, outdir);
        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));
        Runtime.getRuntime().addShutdownHook(hook);
        // Up to this point, catalog has not been modified
        try {
            // Modify cohort status to "CALCULATING"
            updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.CALCULATING);

            VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine());
            VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();

            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(dataStore.getDbName());
            Map<String, Integer> cohortNameIdMap = new HashMap<>(cohortIds.size());
            Map<String, Set<String>> cohortSamplesMap = new HashMap<>(cohortIds.size());
            for (Map.Entry<Long, Cohort> entry : cohortsMap.entrySet()) {
                cohortNameIdMap.put(entry.getValue().getName(), entry.getKey().intValue());
                cohortSamplesMap.put(entry.getValue().getName(), entry.getValue().getSamples()
                        .stream()
                        .map(sampleId -> {
                            return studyConfiguration.getSampleIds().inverse().get(sampleId.intValue());
                        })
                        .collect(Collectors.toSet()));
            }
            URI stats = variantStatisticsManager.createStats(dbAdaptor, outdirUri.resolve(outputFileName), cohortSamplesMap, cohortNameIdMap, studyConfiguration, calculateStatsOptions);

            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job still running. Statistics created."));
            variantStatisticsManager.loadStats(dbAdaptor, stats, studyConfiguration, options);

            if (catalogOutDirId != null) {
                copyResults(Paths.get(outdirUri), catalogOutDirId, sessionId);
            }

            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
            // Modify cohort status to "READY"
            updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.READY);
        } catch (Exception e) {
            // Error!
            logger.error("Error executing stats. Set cohorts status to " + Cohort.CohortStatus.INVALID, e);
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with error : " + e.getMessage()));
            // Modify to "INVALID"
            updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.INVALID);
            throw new AnalysisExecutionException("Error calculating statistics.", e);
        } finally {
            // Remove hook
            Runtime.getRuntime().removeShutdownHook(hook);
        }

    }

    /**
     * Accepts options:
     *      {@link VariantStorageManager.Options#FILE_ID}
     *      {@link VariantStorageManager.Options#UPDATE_STATS}
     *      {@link VariantStorageManager.Options#AGGREGATION_MAPPING_PROPERTIES}
     *      {@link VariantStatisticsManager#OUTPUT_FILE_NAME}
     *      {@link VariantDBAdaptor.VariantQueryParams#REGION}
     *      {@link ExecutorManager#EXECUTE}
     *      {@link ExecutorManager#SIMULATE}
     *      {@link AnalysisFileIndexer#LOG_LEVEL}
     *      {@link AnalysisFileIndexer#PARAMETERS}
     *
     *
     * @param outDirId
     * @param cohortIds
     * @param sessionId
     * @param options
     * @return
     * @throws AnalysisExecutionException
     * @throws CatalogException
     * @throws IOException
     * @deprecated use {@link #calculateStats(List, String, String, String, QueryOptions)}
     */
    @Deprecated
    public QueryResult<Job> calculateStats(Long outDirId, List<Long> cohortIds, String sessionId, QueryOptions options)
            throws AnalysisExecutionException, CatalogException, IOException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(ExecutorManager.EXECUTE);
        final boolean simulate = options.getBoolean(ExecutorManager.SIMULATE);
        String fileIdStr = options.getString(VariantStorageManager.Options.FILE_ID.key(), null);
        boolean updateStats = options.getBoolean(VariantStorageManager.Options.UPDATE_STATS.key(), false);
        final Long fileId = fileIdStr == null ? null : catalogManager.getFileId(fileIdStr, sessionId);
        final long start = System.currentTimeMillis();

        cohortIds = checkAggregated(cohortIds, options);
        Map<Long, Cohort> cohortsMap = checkCanCalculateCohorts(cohortIds, updateStats, sessionId);
        long studyId = catalogManager.getStudyIdByCohortId(cohortIds.get(0));


        String region = options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key());
        String outputFileName = buildOutputFileName(cohortIds, options, cohortsMap, region);

        updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.CALCULATING);


        File outDir;
        if (outDirId == null || outDirId <= 0) {
//            outDir = catalogManager.getFileParent(indexedFileId, null, sessionId).first();
            outDir = catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(), ""), new QueryOptions(),
                    sessionId).first();
        } else {
            outDir = catalogManager.getFile(outDirId, null, sessionId).first();
        }

        /** Create temporal Job Outdir **/
        final String randomString = "I_" + RandomStringUtils.randomAlphanumeric(10);
        final URI temporalOutDirUri;
        if (simulate) {
            temporalOutDirUri = AnalysisFileIndexer.createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        }

        /** create command line **/
        String opencgaAnalysisBinPath = Paths.get(Config.getOpenCGAHome(), "bin", AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME).toString();

        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        StringBuilder sb = new StringBuilder()
                .append(opencgaAnalysisBinPath)
                .append(" variant stats ")
                .append(" --study-id ").append(studyId)
                .append(" --session-id ").append(sessionId)
                .append(" --output-filename ").append(outputFileName)
                .append(" --job-id ").append(randomString)
                ;
        if (fileId != null) {
            sb.append(" --file-id ").append(fileId);
        }
        if (options.containsKey(AnalysisFileIndexer.LOG_LEVEL)) {
            sb.append(" --log-level ").append(options.getString(AnalysisFileIndexer.LOG_LEVEL));
        }
        if (isNotEmpty(region)) {
            sb.append(" --region ").append(region);
        }
        if (updateStats) {
            sb.append(" --update-stats ");
        }

        // if the study is aggregated and a mapping file is provided, pass it to storage 
        // and create in catalog the cohorts described in the mapping file
        Study study = catalogManager.getStudy(studyId, new QueryOptions("include", "projects.studies.attributes"), sessionId).first();
        VariantSource.Aggregation studyAggregation = VariantSource.Aggregation.valueOf(study.getAttributes()
                .getOrDefault(VariantStorageManager.Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.NONE).toString());
        if (VariantSource.Aggregation.isAggregated(studyAggregation)
                && !options.getString(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key()).isEmpty()) {
            sb.append(" --aggregation-mapping-file ")
                    .append(options.getString(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key()));
        }

        if (!cohortsMap.isEmpty()) {
            sb.append(" --cohort-ids ");
            for (Iterator<Long> iterator = cohortsMap.keySet().iterator(); iterator.hasNext(); ) {
                Long cohortId = iterator.next();
                sb.append(cohortId);
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
        }
        if (options.containsKey(AnalysisFileIndexer.PARAMETERS)) {
            List<String> extraParams = options.getAsStringList(AnalysisFileIndexer.PARAMETERS);
            for (String extraParam : extraParams) {
                sb.append(" ").append(extraParam);
            }
        }

        String commandLine = sb.toString();
        logger.debug("CommandLine to calculate stats {}" + commandLine);

        /** Update StudyConfiguration **/
        if (!simulate) {
            updateStudyConfiguration(sessionId, studyId, dataStore);
        }

        /** create job **/
        String jobName = "calculate-stats";
        String jobDescription = "Stats calculation for cohort " + cohortsMap.values().stream().map(Cohort::getName).collect(Collectors.toList());
        HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(Job.TYPE, Job.Type.COHORT_STATS);
        attributes.put("cohortIds", cohortIds);
        HashMap<String, Object> resourceManagerAttributes = new HashMap<>();
        JobFactory jobFactory = new JobFactory(catalogManager);
        return jobFactory.createJob(studyId, jobName,
                AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME, jobDescription, outDir, Collections.emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                attributes, resourceManagerAttributes);
    }

    protected Thread buildHook(List<Long> cohortIds, String sessionId, Path outdir) {
        return new Thread(() -> {
            try {
                // If the status has not been changed by the method and is still running, we assume that the execution failed.
                Job.JobStatus status = readJobStatus(outdir);
                if (status.getName().equalsIgnoreCase(Job.JobStatus.RUNNING)) {
                    writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job finished with an error."));
                    updateCohorts(cohortIds, sessionId, Cohort.CohortStatus.INVALID);
                }
            } catch (IOException | CatalogException e) {
                logger.error("Error modifying " + AbstractExecutor.JOB_STATUS_FILE, e);
            }
        });
    }

    protected String buildOutputFileName(List<Long> cohortIds, QueryOptions options, Map<Long, Cohort> cohortsMap, String region) {
        final String outputFileName;
        if (isNotEmpty(options.getString(VariantStatisticsManager.OUTPUT_FILE_NAME))) {
            outputFileName = options.getString(VariantStatisticsManager.OUTPUT_FILE_NAME);
        } else {
            StringBuilder outputFileNameBuilder;
            outputFileNameBuilder = new StringBuilder("stats_");
            if (isNotEmpty(region)) {
                outputFileNameBuilder.append(region).append("_");
            }
            for (Iterator<Long> iterator = cohortIds.iterator(); iterator.hasNext(); ) {
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

    protected List<Long> checkAggregated(List<Long> cohortIds, QueryOptions options) throws CatalogException {
        if (cohortIds == null || cohortIds.isEmpty()) {
            if (!options.containsKey(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key())) {
                throw new CatalogException("Cohort list empty");
            }
            cohortIds = Collections.emptyList();
        }
        return cohortIds;
    }

    /**
     * Check if a set of given cohorts are available to calculate statistics
     *
     * @param cohortIds     Set of cohorts
     * @param updateStats   Update already existing stats
     * @param sessionId     User's sessionId
     * @throws CatalogException
     */
    protected Map<Long, Cohort> checkCanCalculateCohorts(List<Long> cohortIds, boolean updateStats, String sessionId) throws CatalogException {
        Set<Long> studyIdSet = new HashSet<>();
        Map<Long, Cohort> cohortMap = new HashMap<>(cohortIds.size());
        for (Long cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohort(cohortId, null, sessionId).first();
            long studyId = catalogManager.getStudyIdByCohortId(cohortId);
            studyIdSet.add(studyId);
            switch (cohort.getStatus().getName()) {
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                case Cohort.CohortStatus.READY:
                    if (updateStats) {
                        catalogManager.modifyCohort(cohortId, new ObjectMap("status.name", Cohort.CohortStatus.INVALID), new QueryOptions(), sessionId);
                        break;
                    }
                case Cohort.CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to calculate stats for cohort " +
                            "{ id: " + cohort.getId() + " name: \"" + cohort.getName() + "\" }" +
                            " with status \"" + cohort.getStatus().getName() + "\"");
            }
            cohortMap.put(cohortId, cohort);
//            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, new Query("id", cohort.getSamples()), new QueryOptions(), sessionId);
        }

        // Check that all cohorts are from the same study
        if (studyIdSet.size() != 1) {
            throw new CatalogException("Error: CohortIds are from multiple studies: " + studyIdSet.toString());
        }

        return cohortMap;
    }

    protected void updateCohorts(List<Long> cohortIds, String sessionId, String status) throws CatalogException {
        for (Long cohortId : cohortIds) {
            catalogManager.modifyCohort(cohortId, new ObjectMap("status.name", status), new QueryOptions(), sessionId);
        }
    }

    /**
     * Accepts options:
     *      {@link ExecutorManager#EXECUTE}
     *      {@link ExecutorManager#SIMULATE}
     *      {@link AnalysisFileIndexer#LOG_LEVEL}
     *      {@link AnalysisFileIndexer#PARAMETERS}
     *      {@link AnalysisFileIndexer#CREATE}
     *      {@link AnalysisFileIndexer#LOAD}
     *      {@link VariantDBAdaptor.VariantQueryParams#REGION}
     *      {@link VariantDBAdaptor.VariantQueryParams#GENE}
     *      {@link VariantDBAdaptor.VariantQueryParams#CHROMOSOME}
     *      {@link VariantDBAdaptor.VariantQueryParams#ANNOT_CONSEQUENCE_TYPE}
     *      {@link VariantAnnotationManager#OVERWRITE_ANNOTATIONS}
     *      {@link VariantAnnotationManager#FILE_NAME}
     *      {@link VariantAnnotationManager#ANNOTATION_SOURCE}
     *      {@link VariantAnnotationManager#SPECIES}
     *      {@link VariantAnnotationManager#ASSEMBLY}
     *
     *
     * @param studyId
     * @param outDirId
     * @param sessionId
     * @param options
     * @return
     * @throws CatalogException
     * @throws AnalysisExecutionException
     */
    public QueryResult<Job> annotateVariants(long studyId, long outDirId, String sessionId, QueryOptions options)
            throws CatalogException, AnalysisExecutionException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(ExecutorManager.EXECUTE);
        final boolean simulate = options.getBoolean(ExecutorManager.SIMULATE);
        final long start = System.currentTimeMillis();

        File outDir = catalogManager.getFile(outDirId, null, sessionId).first();
        List<Long> inputFiles = new ArrayList<>();

        /** Create temporal Job Outdir **/
        final URI temporalOutDirUri;
        final String randomString = "I_" + RandomStringUtils.randomAlphanumeric(10);
        if (simulate) {
            temporalOutDirUri = AnalysisFileIndexer.createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        }

        /** create command line **/
        String opencgaAnalysisBinPath = Paths.get(Config.getOpenCGAHome(), "bin", AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME).toString();
        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        StringBuilder sb = new StringBuilder()
                .append(opencgaAnalysisBinPath)
                .append(" variant annotate ")
                .append(" --study-id ").append(studyId)
                .append(" --session-id ").append(sessionId)
                .append(" --job-id ").append(randomString)
                .append(" --outdir-id ").append(outDir.getId());


        if (isNotEmpty(options.getString(AnalysisFileIndexer.LOAD))) {
            String fileIdstr = options.getString(AnalysisFileIndexer.LOAD);
            long fileId = catalogManager.getFileId(fileIdstr);
            if (fileId < 0) {
                throw CatalogDBException.idNotFound("File", fileIdstr);
            }

            sb.append(" --load ").append(fileId);
            inputFiles.add(fileId);
        }

        if (options.getBoolean(AnalysisFileIndexer.CREATE, false)) {
            sb.append(" --create ");
        }

        if (options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS)) {
            sb.append(" --overwrite-annotations ");
        }

        //TODO: Read from Catalog?
        if (isNotEmpty(options.getString(VariantAnnotationManager.SPECIES))) {
            sb.append(" --species ").append(options.getString(VariantAnnotationManager.SPECIES));
        }

        if (isNotEmpty(options.getString(VariantAnnotationManager.ASSEMBLY))) {
            sb.append(" --assembly ").append(options.getString(VariantAnnotationManager.ASSEMBLY));
        }

        if (isNotEmpty(options.getString(VariantAnnotationManager.FILE_NAME))) {
            sb.append(" --output-filename ").append(options.getString(VariantAnnotationManager.FILE_NAME));
        }

        if (isNotEmpty(options.getString(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY))) {
            sb.append(" --custom-name ").append(options.getString(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY));
        }

        if (isNotEmpty(options.getString(VariantAnnotationManager.ANNOTATION_SOURCE))) {
            sb.append(" --annotator ").append(options.getString(VariantAnnotationManager.ANNOTATION_SOURCE));
        }

        if (isNotEmpty(options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key()))) {
            sb.append(" --filter-region ").append(options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key()));
        }

        if (isNotEmpty(options.getString(VariantDBAdaptor.VariantQueryParams.GENE.key()))) {
            sb.append(" --filter-gene ").append(options.getString(VariantDBAdaptor.VariantQueryParams.GENE.key()));
        }

        if (isNotEmpty(options.getString(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key()))) {
            sb.append(" --filter-chromosome ").append(options.getString(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key()));
        }

        if (isNotEmpty(options.getString(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key()))) {
            sb.append(" --filter-annot-consequence-type ").append(options.getString(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key()));
        }

        if (options.containsKey(AnalysisFileIndexer.LOG_LEVEL)) {
            sb.append(" --log-level ").append(options.getString(AnalysisFileIndexer.LOG_LEVEL));
        }
        if (options.containsKey(AnalysisFileIndexer.PARAMETERS)) {
            List<String> extraParams = options.getAsStringList(AnalysisFileIndexer.PARAMETERS);
            for (String extraParam : extraParams) {
                sb.append(" ").append(extraParam);
            }
        }
        String commandLine = sb.toString();
        logger.debug("CommandLine to annotate variants {}", commandLine);

        /** Update StudyConfiguration **/
        if (!simulate) {
            try {
                StudyConfigurationManager studyConfigurationManager = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine())
                        .getDBAdaptor(dataStore.getDbName()).getStudyConfigurationManager();
                new CatalogStudyConfigurationFactory(catalogManager).updateStudyConfigurationFromCatalog(studyId, studyConfigurationManager, sessionId);
            } catch (StorageManagerException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        /** create job **/
        String jobDescription = "Variant annotation";
        String jobName = "annotate-variants";
        JobFactory jobFactory = new JobFactory(catalogManager);
        HashMap<String, Object> resourceManagerAttributes = new HashMap<>();
        resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, randomString);
        return jobFactory.createJob(studyId, jobName,
                AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME, jobDescription, outDir, inputFiles,
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                new HashMap<>(), resourceManagerAttributes);
    }

}
