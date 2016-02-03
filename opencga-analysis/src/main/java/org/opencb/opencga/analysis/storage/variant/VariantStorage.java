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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.execution.AnalysisExecutionException;
import org.opencb.opencga.analysis.execution.AnalysisJobExecutor;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.CatalogStudyConfigurationFactory;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 06/03/15.
 */
public class VariantStorage {

    protected static Logger logger = LoggerFactory.getLogger(VariantStorage.class);

    final CatalogManager catalogManager;

    public VariantStorage(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }


    public QueryResult<Job> calculateStats(Integer outDirId, List<Integer> cohortIds, String sessionId, QueryOptions options)
            throws AnalysisExecutionException, CatalogException, IOException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(AnalysisJobExecutor.EXECUTE);
        final boolean simulate = options.getBoolean(AnalysisJobExecutor.SIMULATE);
        String fileIdStr = options.getString(VariantStorageManager.Options.FILE_ID.key(), null);
        boolean updateStats = options.getBoolean(VariantStorageManager.Options.UPDATE_STATS.key(), false);
        final Integer fileId = fileIdStr == null ? null : catalogManager.getFileId(fileIdStr);
        final long start = System.currentTimeMillis();

        if ((cohortIds == null || cohortIds.isEmpty()) 
                && !options.containsKey(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key())) {
            throw new CatalogException("Cohort list empty");
        }

        StringBuilder outputFileName = new StringBuilder();
        Map<Cohort, List<Sample>> cohorts = new HashMap<>(cohortIds.size());
        Set<Integer> studyIdSet = new HashSet<>();
        Map<Integer, Cohort> cohortMap = new HashMap<>(cohortIds.size());

        for (Integer cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohort(cohortId, null, sessionId).first();
            int studyId = catalogManager.getStudyIdByCohortId(cohortId);
            studyIdSet.add(studyId);
            switch (cohort.getStatus()) {
                case NONE:
                case INVALID:
                    break;
                case READY:
                    if (updateStats) {
                        catalogManager.modifyCohort(cohortId, new ObjectMap("status", Cohort.Status.INVALID), sessionId);
                        break;
                    }
                case CALCULATING:
                    throw new CatalogException("Unable to calculate stats for cohort " +
                            "{ id: " + cohort.getId() + " name: \"" + cohort.getName() + "\" }" +
                            " with status \"" + cohort.getStatus() + "\"");
            }
            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, new QueryOptions("id", cohort.getSamples()), sessionId);
            cohorts.put(cohort, sampleQueryResult.getResult());
            cohortMap.put(cohortId, cohort);
        }
        for (Integer cohortId : cohortIds) {
            if (outputFileName.length() > 0) {
                outputFileName.append('_');
            }
            outputFileName.append(cohortMap.get(cohortId).getName());

            /** Modify cohort status to "CALCULATING" **/
            catalogManager.modifyCohort(cohortId, new ObjectMap("status", Cohort.Status.CALCULATING), sessionId);

        }

        // Check that all cohorts are from the same study
        int studyId;
        if (studyIdSet.size() == 1) {
            studyId = studyIdSet.iterator().next();
        } else {
            throw new CatalogException("Error: CohortIds are from multiple studies: " + studyIdSet.toString());
        }

        File outDir;
        if (outDirId == null || outDirId <= 0) {
//            outDir = catalogManager.getFileParent(indexedFileId, null, sessionId).first();
            outDir = catalogManager.getAllFiles(studyId, new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), ""), sessionId).first();
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
        String opencgaStorageBinPath = Paths.get(Config.getOpenCGAHome(), "bin", AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME).toString();

        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        StringBuilder sb = new StringBuilder()

                .append(opencgaStorageBinPath)
                .append(" stats-variants ")
                .append(" --storage-engine ").append(dataStore.getStorageEngine())
                .append(" --study-id ").append(studyId)
                .append(" --output-filename ").append(temporalOutDirUri.resolve("stats_" + outputFileName).toString())
                .append(" --database ").append(dataStore.getDbName())
//                .append(" -D").append(VariantStorageManager.Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key()).append("=").append(CatalogStudyConfigurationManager.class.getName())
//                .append(" -D").append("sessionId").append("=").append(sessionId)
//                .append(" --cohort-name ").append(cohort.getId())
//                .append(" --cohort-samples ")
                ;
        if (fileId != null) {
            sb.append(" --file-id ").append(fileId);
        }
        if (options.containsKey(AnalysisFileIndexer.LOG_LEVEL)) {
            sb.append(" --log-level ").append(options.getString(AnalysisFileIndexer.LOG_LEVEL));
        }
        if (updateStats) {
            sb.append(" --update-stats ");
        }

        // if the study is aggregated and a mapping file is provided, pass it to storage 
        // and create in catalog the cohorts described in the mapping file
        Study study = catalogManager.getStudy(studyId, sessionId, new QueryOptions("include", "projects.studies.attributes")).first();
        VariantSource.Aggregation studyAggregation = VariantSource.Aggregation.valueOf(study.getAttributes()
                .getOrDefault(VariantStorageManager.Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.NONE).toString());
        if (VariantSource.Aggregation.isAggregated(studyAggregation)
                && options.containsKey(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key())) {
            sb.append(" --aggregation-mapping-file ")
                    .append(options.getString(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key()));
        }
        
        for (Map.Entry<Cohort, List<Sample>> entry : cohorts.entrySet()) {
            sb.append(" --cohort-sample-ids ").append(entry.getKey().getName());
            sb.append(":");
//            for (Sample sample : entry.getValue()) {
//                sb.append(sample.getName()).append(",");
//            }
//            sb.append(" --cohort-ids ").append(entry.getKey().getName()).append(":").append(entry.getKey().getId());
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
            try {
                StudyConfigurationManager studyConfigurationManager = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine())
                        .getDBAdaptor(dataStore.getDbName()).getStudyConfigurationManager();
                new CatalogStudyConfigurationFactory(catalogManager).updateStudyConfigurationFromCatalog(studyId, studyConfigurationManager, sessionId);
            } catch (StorageManagerException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        /** create job **/
        String jobName = "calculate-stats";
        String jobDescription = "Stats calculation for cohort " + cohortIds;
        HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(Job.TYPE, Job.Type.COHORT_STATS);
        attributes.put("cohortIds", cohortIds);
        return AnalysisJobExecutor.createJob(catalogManager, studyId, jobName,
                AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.<Integer>emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                attributes, new HashMap<>());
    }

    public QueryResult<Job> annotateVariants(int studyId, int outDirId, String sessionId, QueryOptions options) throws CatalogException, AnalysisExecutionException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(AnalysisJobExecutor.EXECUTE);
        final boolean simulate = options.getBoolean(AnalysisJobExecutor.SIMULATE);
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
        String opencgaStorageBinPath = Paths.get(Config.getOpenCGAHome(), "bin", AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME).toString();
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        StringBuilder sb = new StringBuilder()
                .append(opencgaStorageBinPath)
                .append(" annotate-variants ")
                .append(" --storage-engine ").append(dataStore.getStorageEngine())
                .append(" --outdir ").append(temporalOutDirUri.toString())
                .append(" --database ").append(dataStore.getDbName());
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
        String jobName = "annotate-stats";
        return AnalysisJobExecutor.createJob(catalogManager, studyId, jobName,
                AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.<Integer>emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                new HashMap<String, Object>(), new HashMap<String, Object>());
    }

}
