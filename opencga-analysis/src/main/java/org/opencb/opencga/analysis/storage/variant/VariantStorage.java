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

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


    public QueryResult<Job> calculateStats(int indexedFileId, Integer outDirId, List<Integer> cohortIds, String sessionId, QueryOptions options)
            throws AnalysisExecutionException, CatalogException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(AnalysisJobExecuter.EXECUTE);
        final boolean simulate = options.getBoolean(AnalysisJobExecuter.SIMULATE);
        final long start = System.currentTimeMillis();

        File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
        int studyId = catalogManager.getStudyIdByFileId(indexedFile.getId());
        if (indexedFile.getType() != File.Type.FILE || indexedFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new AnalysisExecutionException("Expected file with {type: FILE, bioformat: VARIANT}. " +
                    "Got {type: " + indexedFile.getType() + ", bioformat: " + indexedFile.getBioformat() + "}");
        }

        StringBuilder outputFileName = new StringBuilder();
        Map<Cohort, List<Sample>> cohorts = new HashMap<>(cohortIds.size());
        for (Integer cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohort(cohortId, null, sessionId).first();
            QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, new QueryOptions("id", cohort.getSamples()), sessionId);
            cohorts.put(cohort, sampleQueryResult.getResult());
            if (outputFileName.length() > 0) {
                outputFileName.append('_');
            }
            outputFileName.append(cohort.getName());
        }

        File outDir;
        if (outDirId == null || outDirId <= 0) {
            outDir = catalogManager.getFileParent(indexedFileId, null, sessionId).first();
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

        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, indexedFile, sessionId);
        StringBuilder sb = new StringBuilder()

                .append(opencgaStorageBinPath)
                .append(" --storage-engine ").append(dataStore.getStorageEngine())
                .append(" stats-variants ")
                .append(" --file-id ").append(indexedFile.getId())
                .append(" --output-filename ").append(temporalOutDirUri.resolve("stats_" + outputFileName).toString())
//                .append(" --study-id ").append(studyId)
                .append(" --database ").append(dataStore.getDbName())
//                .append(" --cohort-name ").append(cohort.getId())
//                .append(" --cohort-samples ")
                ;
        for (Map.Entry<Cohort, List<Sample>> entry : cohorts.entrySet()) {
            sb.append(" --cohort ").append(entry.getKey().getName()).append(":");
            for (Sample sample : entry.getValue()) {
                sb.append(sample.getName()).append(",");
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

        /** create job **/
        String jobName = "calculate-stats";
        String jobDescription = "Stats calculation for cohort " + cohortIds;
        return AnalysisJobExecuter.createJob(catalogManager, studyId, jobName,
                AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.<Integer>emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                new HashMap<String, Object>(), new HashMap<String, Object>());
    }

    public QueryResult<Job> annotateVariants(int indexedFileId, Integer outDirId, String sessionId, QueryOptions options) throws CatalogException, AnalysisExecutionException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(AnalysisJobExecuter.EXECUTE);
        final boolean simulate = options.getBoolean(AnalysisJobExecuter.SIMULATE);
        final long start = System.currentTimeMillis();

        File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
        int studyId = catalogManager.getStudyIdByFileId(indexedFile.getId());
        if (indexedFile.getType() != File.Type.FILE || indexedFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new AnalysisExecutionException("Expected file with {type: FILE, bioformat: VARIANT}. " +
                    "Got {type: " + indexedFile.getType() + ", bioformat: " + indexedFile.getBioformat() + "}");
        }

        File outDir;
        if (outDirId == null || outDirId <= 0) {
            outDir = catalogManager.getFileParent(indexedFileId, null, sessionId).first();
        } else {
            outDir = catalogManager.getFile(outDirId, null, sessionId).first();
        }

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
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, indexedFile, sessionId);

        StringBuilder sb = new StringBuilder()
                .append(opencgaStorageBinPath)
                .append(" --storage-engine ").append(dataStore.getStorageEngine())
                .append(" annotate-variants ")
                .append(" --outdir ").append(temporalOutDirUri.toString())
                .append(" --database ").append(dataStore.getDbName())
                ;
        if (options.containsKey(AnalysisFileIndexer.PARAMETERS)) {
            List<String> extraParams = options.getAsStringList(AnalysisFileIndexer.PARAMETERS);
            for (String extraParam : extraParams) {
                sb.append(" ").append(extraParam);
            }
        }
        String commandLine = sb.toString();
        logger.debug("CommandLine to annotate variants {}", commandLine);

        /** create job **/
        String jobDescription = "Variant annotation";
        String jobName = "annotate-stats";
        return AnalysisJobExecuter.createJob(catalogManager, studyId, jobName,
                AnalysisFileIndexer.OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.<Integer>emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate,
                new HashMap<String, Object>(), new HashMap<String, Object>());
    }

}
