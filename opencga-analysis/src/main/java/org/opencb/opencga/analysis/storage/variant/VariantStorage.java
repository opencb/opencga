package org.opencb.opencga.analysis.storage.variant;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;
import org.opencb.opencga.lib.exec.Command;
import org.opencb.opencga.lib.exec.SingleProcess;
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
    public static final String OPENCGA_STORAGE_BIN_NAME = "opencga-storage.sh";

    final CatalogManager catalogManager;

    public VariantStorage(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }


    public QueryResult<Job> calculateStats(int indexFileId, List<Integer> cohortIds, String sessionId, QueryOptions options) throws CatalogException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean("execute");
        final boolean simulate = options.getBoolean("simulate");
        final long start = System.currentTimeMillis();

        File indexFile = catalogManager.getFile(indexFileId, sessionId).first();
        int studyId = catalogManager.getStudyIdByFileId(indexFile.getId());
        if (indexFile.getType() != File.Type.INDEX || indexFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new CatalogException("Expected file with {type: INDEX, bioformat: VARIANT}. " +
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

        int outDirId = catalogManager.getFileParent(indexFileId, null, sessionId).first().getId();


        /** Create temporal Job Outdir **/
        final String randomString = "I_" + StringUtils.randomString(10);
        final URI temporalOutDirUri;
        if (simulate) {
            temporalOutDirUri = AnalysisFileIndexer.createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        }

        /** create command line **/
        String opencgaStorageBinPath = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_STORAGE_BIN_NAME).toString();

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

        String commandLine = sb.toString();
        logger.debug("CommandLine to calculate stats {}" + commandLine);

        String jobDescription = "Stats calculation for cohort " + cohortIds;
        String jobName = "calculate-stats";
        final QueryResult<Job> jobQueryResult;
        if (simulate) {
            jobQueryResult = new QueryResult<Job>("simulatedStatsVariant", (int)(System.currentTimeMillis()-start), 1, 1, "", "", Collections.singletonList(
                    new Job(-10, jobName, catalogManager.getUserIdBySessionId(sessionId), OPENCGA_STORAGE_BIN_NAME,
                            TimeUtils.getTime(), jobDescription, start, System.currentTimeMillis(), "", commandLine, -1,
                            Job.Status.DONE, -1, outDirId, temporalOutDirUri, Collections.<Integer>emptyList(), Collections.<Integer>emptyList(),
                            null, null, null)));
        } else {
            if (execute) {

//            URI out = temporalOutDirUri.resolve("job_out." + job.getId() + ".log");
//            URI err = temporalOutDirUri.resolve("job_err." + job.getId() + ".log");

                Command com = new Command(commandLine);
                SingleProcess sp = new SingleProcess(com);
                sp.getRunnableProcess().run();
                jobQueryResult = catalogManager.createJob(studyId, jobName, OPENCGA_STORAGE_BIN_NAME, jobDescription, commandLine, temporalOutDirUri,
                        outDirId, Collections.<Integer>emptyList(), null, Job.Status.DONE, null, sessionId);

            } else {
                ObjectMap jobResourceManagerAttributes = new ObjectMap(Job.JOB_SCHEDULER_NAME, randomString);
                jobQueryResult = catalogManager.createJob(studyId, jobName, OPENCGA_STORAGE_BIN_NAME, jobDescription, commandLine, temporalOutDirUri,
                        outDirId, Collections.<Integer>emptyList(), jobResourceManagerAttributes, options, sessionId);
            }
        }

        return jobQueryResult;
    }

    public QueryResult<Job> annotateVariants(int indexFileId, String sessionId, QueryOptions options) throws CatalogException {
        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean("execute");
        final boolean simulate = options.getBoolean("simulate");
        final long start = System.currentTimeMillis();

        File indexFile = catalogManager.getFile(indexFileId, sessionId).first();
        int studyId = catalogManager.getStudyIdByFileId(indexFile.getId());
        if (indexFile.getType() != File.Type.INDEX || indexFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new CatalogException("Expected file with {type: INDEX, bioformat: VARIANT}. " +
                    "Got {type: " + indexFile.getType() + ", bioformat: " + indexFile.getBioformat() + "}");
        }

        int outDirId = catalogManager.getFileParent(indexFileId, null, sessionId).first().getId();


        /** Create temporal Job Outdir **/
        final URI temporalOutDirUri;
        final String randomString = "I_" + StringUtils.randomString(10);
        if (simulate) {
            temporalOutDirUri = AnalysisFileIndexer.createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);
        }

        /** create command line **/
        String opencgaStorageBinPath = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_STORAGE_BIN_NAME).toString();

        StringBuilder sb = new StringBuilder()
                .append(opencgaStorageBinPath)
                .append(" --storage-engine ").append(indexFile.getAttributes().get(AnalysisFileIndexer.STORAGE_ENGINE))
                .append(" annotate-variants ")
                .append(" --outdir ").append(temporalOutDirUri.toString())
                .append(" --database ").append(indexFile.getAttributes().get(AnalysisFileIndexer.DB_NAME))
                ;
        if (options.containsKey("parameters")) {
            Map<String, Object> parameters = options.getMap("parameters");
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                sb.append(entry.getKey())
                        .append(" ")
                        .append(entry.getValue());
            }
        }


        String commandLine = sb.toString();
        logger.debug("CommandLine to annotate variants {}" + commandLine);

        String jobDescription = "Variant annotation";
        String jobName = "annotate-stats";
        QueryResult<Job> jobQueryResult;
        if (simulate) {
            logger.info("CommandLine to annotate variants {}" + commandLine);
            return new QueryResult<>("simulateJob", (int)(System.currentTimeMillis() - start), 1, 1, "", "", Collections.singletonList(
                    new Job(-10, jobName, catalogManager.getUserIdBySessionId(sessionId), OPENCGA_STORAGE_BIN_NAME,
                            jobDescription, commandLine, start, System.currentTimeMillis(), null, commandLine, -1,
                            Job.Status.DONE, -1, outDirId, temporalOutDirUri, Collections.<Integer>emptyList(), Collections.<Integer>emptyList(),
                            Collections.<String>emptyList(), null, null)));
        } else {
            Map<String, Object> resourceManagerAttributes = new HashMap<>();
            resourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, randomString);
            if (execute) {

//              URI out = temporalOutDirUri.resolve("job_out." + job.getId() + ".log");
//              URI err = temporalOutDirUri.resolve("job_err." + job.getId() + ".log");

                Command com = new Command(commandLine);
                SingleProcess sp = new SingleProcess(com);
                sp.getRunnableProcess().run();
                jobQueryResult = catalogManager.createJob(studyId, jobName, OPENCGA_STORAGE_BIN_NAME, jobDescription, commandLine, temporalOutDirUri,
                        outDirId, Collections.<Integer>emptyList(), resourceManagerAttributes, Job.Status.DONE, null, sessionId);

            } else {
                jobQueryResult = catalogManager.createJob(studyId, jobName, OPENCGA_STORAGE_BIN_NAME, jobDescription, commandLine, temporalOutDirUri,
                        outDirId, Collections.<Integer>emptyList(), null, options, sessionId);
            }
            return jobQueryResult;
        }

    }

}
