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
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by hpccoll1 on 06/03/15.
 */
public class VariantStorage {

    protected static Logger logger = LoggerFactory.getLogger(VariantStorage.class);

    final CatalogManager catalogManager;

    public VariantStorage(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }


    public QueryResult<Job> calculateStats(int indexFileId, List<Integer> cohortIds, String sessionId, QueryOptions options)
            throws AnalysisExecutionException, CatalogException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean("execute");
        final boolean simulate = options.getBoolean("simulate");
        final boolean recordOutput = options.getBoolean("recordOutput");
        final long start = System.currentTimeMillis();

        File indexFile = catalogManager.getFile(indexFileId, sessionId).first();
        int studyId = catalogManager.getStudyIdByFileId(indexFile.getId());
        if (indexFile.getType() != File.Type.INDEX || indexFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new AnalysisExecutionException("Expected file with {type: INDEX, bioformat: VARIANT}. " +
                    "Got {type: " + indexFile.getType() + ", bioformat: " + indexFile.getBioformat() + "}");
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

        File outDir = catalogManager.getFileParent(indexFileId, null, sessionId).first();

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

        Object dbName = indexFile.getAttributes().get(AnalysisFileIndexer.DB_NAME);
        StringBuilder sb = new StringBuilder()

                .append(opencgaStorageBinPath)
                .append(" --storage-engine ").append(indexFile.getAttributes().get(AnalysisFileIndexer.STORAGE_ENGINE))
                .append(" stats-variants ")
                .append(" --file-id ").append(indexFile.getId())
                .append(" --output-filename ").append(temporalOutDirUri.resolve("stats_" + outputFileName).toString())
                .append(" --study-id ").append(studyId)
                .append(" --database ").append(dbName)
//                .append(" --cohort-name ").append(cohort.getId())
//                .append(" --cohort-samples ")
                ;
        for (Map.Entry<Cohort, List<Sample>> entry : cohorts.entrySet()) {
            sb.append(" -C ").append(entry.getKey().getName()).append(":");
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
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate, recordOutput, new HashMap<String, Object>());
    }

    public QueryResult<Job> annotateVariants(int indexFileId, String sessionId, QueryOptions options) throws CatalogException, AnalysisExecutionException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean("execute");
        final boolean simulate = options.getBoolean("simulate");
        final boolean recordOutput = options.getBoolean("recordOutput");
        final long start = System.currentTimeMillis();

        File indexFile = catalogManager.getFile(indexFileId, sessionId).first();
        int studyId = catalogManager.getStudyIdByFileId(indexFile.getId());
        if (indexFile.getType() != File.Type.INDEX || indexFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new AnalysisExecutionException("Expected file with {type: INDEX, bioformat: VARIANT}. " +
                    "Got {type: " + indexFile.getType() + ", bioformat: " + indexFile.getBioformat() + "}");
        }

        File outDir = catalogManager.getFileParent(indexFileId, null, sessionId).first();


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

        StringBuilder sb = new StringBuilder()
                .append(opencgaStorageBinPath)
                .append(" --storage-engine ").append(indexFile.getAttributes().get(AnalysisFileIndexer.STORAGE_ENGINE))
                .append(" annotate-variants ")
                .append(" --outdir ").append(temporalOutDirUri.toString())
                .append(" --database ").append(indexFile.getAttributes().get(AnalysisFileIndexer.DB_NAME))
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
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate, recordOutput, new HashMap<String, Object>());
    }

}
