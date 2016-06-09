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

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.JobFactory;
import org.opencb.opencga.analysis.execution.executors.ExecutorManager;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.CatalogStudyConfigurationFactory;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Created by jacobo on 06/03/15.
 */
public class VariantStorage {

    protected static Logger logger = LoggerFactory.getLogger(VariantStorage.class);

    final CatalogManager catalogManager;

    public VariantStorage(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
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
     */
    public QueryResult<Job> calculateStats(Long outDirId, List<Long> cohortIds, String sessionId, QueryOptions options)
            throws AnalysisExecutionException, CatalogException, IOException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(ExecutorManager.EXECUTE);
        final boolean simulate = options.getBoolean(ExecutorManager.SIMULATE);
        String fileIdStr = options.getString(VariantStorageManager.Options.FILE_ID.key(), null);
        boolean updateStats = options.getBoolean(VariantStorageManager.Options.UPDATE_STATS.key(), false);
        final Long fileId = fileIdStr == null ? null : catalogManager.getFileId(fileIdStr);
        final long start = System.currentTimeMillis();

        if (cohortIds == null || cohortIds.isEmpty()) {
            if (!options.containsKey(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key())) {
                throw new CatalogException("Cohort list empty");
            }
            cohortIds = Collections.emptyList();
        }

        Map<Cohort, List<Sample>> cohorts = new HashMap<>(cohortIds.size());
        Set<Long> studyIdSet = new HashSet<>();
        Map<Long, Cohort> cohortMap = new HashMap<>(cohortIds.size());

        for (Long cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohort(cohortId, null, sessionId).first();
            long studyId = catalogManager.getStudyIdByCohortId(cohortId);
            studyIdSet.add(studyId);
            switch (cohort.getStatus().getStatus()) {
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                case Cohort.CohortStatus.READY:
                    if (updateStats) {
                        catalogManager.modifyCohort(cohortId, new ObjectMap("status.status", Cohort.CohortStatus.INVALID), new QueryOptions(), sessionId);
                        break;
                    }
                case Cohort.CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to calculate stats for cohort " +
                            "{ id: " + cohort.getId() + " name: \"" + cohort.getName() + "\" }" +
                            " with status \"" + cohort.getStatus().getStatus() + "\"");
            }
            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, new Query("id", cohort.getSamples()), new QueryOptions(), sessionId);
            cohorts.put(cohort, sampleQueryResult.getResult());
            cohortMap.put(cohortId, cohort);
        }

        String region = options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key());
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
                outputFileNameBuilder.append(cohortMap.get(cohortId).getName());
                if (iterator.hasNext()) {
                    outputFileNameBuilder.append('_');
                }
            }
            outputFileName = outputFileNameBuilder.toString();
        }

        for (Long cohortId : cohortIds) {
            /** Modify cohort status to "CALCULATING" **/
            catalogManager.modifyCohort(cohortId, new ObjectMap("status.status", Cohort.CohortStatus.CALCULATING), new QueryOptions(), sessionId);
        }

        // Check that all cohorts are from the same study
        long studyId;
        if (studyIdSet.size() == 1) {
            studyId = studyIdSet.iterator().next();
        } else {
            throw new CatalogException("Error: CohortIds are from multiple studies: " + studyIdSet.toString());
        }

        File outDir;
        if (outDirId == null || outDirId <= 0) {
//            outDir = catalogManager.getFileParent(indexedFileId, null, sessionId).first();
            outDir = catalogManager.getAllFiles(studyId, new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), ""), new QueryOptions(),
                    sessionId).first();
        } else {
            outDir = catalogManager.getFile(outDirId, null, sessionId).first();
        }

        /** Create temporal Job Outdir **/
        final String randomString = "I_" + StringUtils.randomString(10);
        final URI temporalOutDirUri;
        if (simulate) {
            temporalOutDirUri = AnalysisFileIndexer.createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        }

        /** create command line **/
        String opencgaAnalysisBinPath = Paths.get(Config.getOpenCGAHome(), "bin", AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME).toString();

        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

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

        if (!cohorts.isEmpty()) {
            sb.append(" --cohort-ids ");
            for (Iterator<Map.Entry<Cohort, List<Sample>>> iterator = cohorts.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<Cohort, List<Sample>> entry = iterator.next();
                sb.append(entry.getKey().getName());
                if (iterator.hasNext()) {
                    sb.append(',');
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
            try (VariantDBAdaptor dbAdaptor = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine())
                    .getDBAdaptor(dataStore.getDbName());
                 StudyConfigurationManager studyConfigurationManager = dbAdaptor.getStudyConfigurationManager()){
                new CatalogStudyConfigurationFactory(catalogManager)
                        .updateStudyConfigurationFromCatalog(studyId, studyConfigurationManager, sessionId);
            } catch (StorageManagerException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new AnalysisExecutionException("Unable to update StudyConfiguration", e);
            }
        }

        /** create job **/
        String jobName = "calculate-stats";
        String jobDescription = "Stats calculation for cohort " + cohortMap.values().stream().map(Cohort::getName).collect(Collectors.toList());
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

    /**
     *
     * Accepts options:
     *      {@link ExecutorManager#EXECUTE}
     *      {@link ExecutorManager#SIMULATE}
     *      {@link AnalysisFileIndexer#LOG_LEVEL}
     *      {@link AnalysisFileIndexer#PARAMETERS}
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
    public QueryResult<Job> annotateVariants(long studyId, long outDirId, String sessionId, QueryOptions options) throws CatalogException, AnalysisExecutionException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(ExecutorManager.EXECUTE);
        final boolean simulate = options.getBoolean(ExecutorManager.SIMULATE);
        final long start = System.currentTimeMillis();

        File outDir = catalogManager.getFile(outDirId, null, sessionId).first();

        /** Create temporal Job Outdir **/
        final URI temporalOutDirUri;
        final String randomString = "I_" + StringUtils.randomString(10);
        if (simulate) {
            temporalOutDirUri = AnalysisFileIndexer.createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        }

        /** create command line **/
        String opencgaAnalysisBinPath = Paths.get(Config.getOpenCGAHome(), "bin", AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME).toString();
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        StringBuilder sb = new StringBuilder()
                .append(opencgaAnalysisBinPath)
                .append(" variant annotate ")
                .append(" --study-id ").append(studyId)
                .append(" --session-id ").append(sessionId)
                .append(" --job-id ").append(randomString)
                .append(" --outdir-id ").append(outDir.getId());


        if (options.getBoolean(VariantAnnotationManager.OVERWRITE_ANNOTATIONS)) {
            sb.append(" --overwrite-annotations ");
        }

        //TODO: Read from Catalog?
        if (!options.getString(VariantAnnotationManager.SPECIES).isEmpty()) {
            sb.append(" --species ").append(options.getString(VariantAnnotationManager.SPECIES));
        }

        if (!options.getString(VariantAnnotationManager.ASSEMBLY).isEmpty()) {
            sb.append(" --assembly ").append(options.getString(VariantAnnotationManager.ASSEMBLY));
        }

        if (!options.getString(VariantAnnotationManager.FILE_NAME).isEmpty()) {
            sb.append(" --output-filename ").append(options.getString(VariantAnnotationManager.FILE_NAME));
        }

        if (!options.getString(VariantAnnotationManager.ANNOTATION_SOURCE).isEmpty()) {
            sb.append(" --annotator ").append(options.getString(VariantAnnotationManager.ANNOTATION_SOURCE));
        }

        if (!options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key()).isEmpty()) {
            sb.append(" --filter-region ").append(options.getString(VariantDBAdaptor.VariantQueryParams.REGION.key()));
        }

        if (!options.getString(VariantDBAdaptor.VariantQueryParams.GENE.key()).isEmpty()) {
            sb.append(" --filter-gene ").append(options.getString(VariantDBAdaptor.VariantQueryParams.GENE.key()));
        }

        if (!options.getString(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key()).isEmpty()) {
            sb.append(" --filter-chromosome ").append(options.getString(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key()));
        }

        if (!options.getString(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key()).isEmpty()) {
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
                AnalysisFileIndexer.OPENCGA_ANALYSIS_BIN_NAME, jobDescription, outDir, Collections.emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                new HashMap<>(), resourceManagerAttributes);
    }

}
