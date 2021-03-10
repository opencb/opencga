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

package org.opencb.opencga.master.monitor.daemons;

import com.google.common.base.CaseFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.ClientProperties;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysis;
import org.opencb.opencga.analysis.cohort.CohortIndexTask;
import org.opencb.opencga.analysis.cohort.CohortTsvAnnotationLoader;
import org.opencb.opencga.analysis.family.FamilyIndexTask;
import org.opencb.opencga.analysis.family.FamilyTsvAnnotationLoader;
import org.opencb.opencga.analysis.family.qc.FamilyQcAnalysis;
import org.opencb.opencga.analysis.file.*;
import org.opencb.opencga.analysis.individual.IndividualIndexTask;
import org.opencb.opencga.analysis.individual.IndividualTsvAnnotationLoader;
import org.opencb.opencga.analysis.individual.qc.IndividualQcAnalysis;
import org.opencb.opencga.analysis.job.JobIndexTask;
import org.opencb.opencga.analysis.sample.SampleIndexTask;
import org.opencb.opencga.analysis.sample.SampleTsvAnnotationLoader;
import org.opencb.opencga.analysis.sample.qc.SampleQcAnalysis;
import org.opencb.opencga.analysis.tools.ToolFactory;
import org.opencb.opencga.analysis.variant.VariantExportTool;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.inferredSex.InferredSexAnalysis;
import org.opencb.opencga.analysis.variant.julie.JulieTool;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.mendelianError.MendelianErrorAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.operations.*;
import org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleVariantFilterAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.*;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.file.FileAclParams;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.core.tools.result.Status;
import org.opencb.opencga.master.monitor.models.PrivateJobUpdateParams;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.SET;
import static org.opencb.opencga.core.api.ParamConstants.JOB_PARAM;
import static org.opencb.opencga.core.api.ParamConstants.STUDY_PARAM;

/**
 * Created by imedina on 16/06/16.
 */
public class ExecutionDaemon extends MonitorParentDaemon {

    public static final String OUTDIR_PARAM = "outdir";
    public static final int EXECUTION_RESULT_FILE_EXPIRATION_MINUTES = 10;
    public static final String REDACTED_TOKEN = "xxxxxxxxxxxxxxxxxxxxx";
    private String internalCli;
    private JobManager jobManager;
    private FileManager fileManager;
    private final Map<String, Long> jobsCountByType = new HashMap<>();
    private final Map<String, Long> retainedLogsTime = new HashMap<>();

    private Path defaultJobDir;

    private static final Map<String, String> TOOL_CLI_MAP;

    // Maximum number of jobs of each type (Pending, queued, running) that will be handled on each iteration.
    // Example: If there are 100 pending jobs, 15 queued, 70 running.
    // On first iteration, it will queue 50 out of the 100 pending jobs. It will check up to 50 queue-running changes out of the 65
    // (15 + 50 from pending), and it will check up to 50 finished jobs from the running ones.
    // On second iteration, it will queue the remaining 50 pending jobs, and so on...
    private static final int NUM_JOBS_HANDLED = 50;
    private final Query pendingJobsQuery;
    private final Query queuedJobsQuery;
    private final Query runningJobsQuery;
    private final QueryOptions queryOptions;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    static {
        TOOL_CLI_MAP = new HashMap<String, String>(){{
            put(FileUnlinkTask.ID, "files unlink");
            put(FileDeleteTask.ID, "files delete");
            put(FetchAndRegisterTask.ID, "files fetch");
            put(FileIndexTask.ID, "files secondary-index");
            put(FileTsvAnnotationLoader.ID, "files tsv-load");
            put(PostLinkSampleAssociation.ID, "files postlink");

            put(SampleIndexTask.ID, "samples secondary-index");
            put(SampleTsvAnnotationLoader.ID, "samples tsv-load");

            put(IndividualIndexTask.ID, "individuals secondary-index");
            put(IndividualTsvAnnotationLoader.ID, "individuals tsv-load");

            put(CohortIndexTask.ID, "cohorts secondary-index");
            put(CohortTsvAnnotationLoader.ID, "cohorts tsv-load");

            put(FamilyIndexTask.ID, "families secondary-index");
            put(FamilyTsvAnnotationLoader.ID, "families tsv-load");

            put(JobIndexTask.ID, "jobs secondary-index");

            put("alignment-index-run", "alignment index-run");
            put("alignment-coverage-run", "alignment coverage-run");
            put("alignment-stats-run", "alignment stats-run");
            put(BwaWrapperAnalysis.ID, "alignment " + BwaWrapperAnalysis.ID + "-run");
            put(SamtoolsWrapperAnalysis.ID, "alignment " + SamtoolsWrapperAnalysis.ID + "-run");
            put(DeeptoolsWrapperAnalysis.ID, "alignment " + DeeptoolsWrapperAnalysis.ID + "-run");
            put(FastqcWrapperAnalysis.ID, "alignment " + FastqcWrapperAnalysis.ID + "-run");
            put(PicardWrapperAnalysis.ID, "alignment " + PicardWrapperAnalysis.ID + "-run");

            put(VariantIndexOperationTool.ID, "variant index-run");
            put(VariantExportTool.ID, "variant export-run");
            put(VariantStatsAnalysis.ID, "variant stats-run");
            put("variant-stats-export", "variant stats-export-run");
            put(SampleVariantStatsAnalysis.ID, "variant sample-stats-run");
            put(CohortVariantStatsAnalysis.ID, "variant cohort-stats-run");
            put(GwasAnalysis.ID, "variant gwas-run");
            put(PlinkWrapperAnalysis.ID, "variant " + PlinkWrapperAnalysis.ID + "-run");
            put(RvtestsWrapperAnalysis.ID, "variant " + RvtestsWrapperAnalysis.ID + "-run");
            put(GatkWrapperAnalysis.ID, "variant " + GatkWrapperAnalysis.ID + "-run");
            put(VariantFileDeleteOperationTool.ID, "variant file-delete");
            put(VariantSecondaryIndexOperationTool.ID, "variant secondary-index");
            put(VariantSecondaryIndexSamplesDeleteOperationTool.ID, "variant secondary-index-delete");
            put(VariantScoreDeleteOperationTool.ID, "variant score-delete");
            put(VariantScoreIndexOperationTool.ID, "variant score-index");
            put(VariantSampleIndexOperationTool.ID, "variant sample-index");
            put(VariantFamilyIndexOperationTool.ID, "variant family-index");
            put(VariantAggregateFamilyOperationTool.ID, "variant aggregate-family");
            put(VariantAggregateOperationTool.ID, "variant aggregate");
            put(VariantAnnotationIndexOperationTool.ID, "variant annotation-index");
            put(VariantAnnotationDeleteOperationTool.ID, "variant annotation-delete");
            put(VariantAnnotationSaveOperationTool.ID, "variant annotation-save");
            put(SampleVariantFilterAnalysis.ID, "variant sample-run");
            put(KnockoutAnalysis.ID, "variant knockout-run");
            put(SampleEligibilityAnalysis.ID, "variant " + SampleEligibilityAnalysis.ID + "-run");
            put(MutationalSignatureAnalysis.ID, "variant " + MutationalSignatureAnalysis.ID + "-run");
            put(MendelianErrorAnalysis.ID, "variant " + MendelianErrorAnalysis.ID + "-run");
            put(InferredSexAnalysis.ID, "variant " + InferredSexAnalysis.ID + "-run");
            put(RelatednessAnalysis.ID, "variant " + RelatednessAnalysis.ID + "-run");
            put(FamilyQcAnalysis.ID, "variant " + FamilyQcAnalysis.ID + "-run");
            put(IndividualQcAnalysis.ID, "variant " + IndividualQcAnalysis.ID + "-run");
            put(SampleQcAnalysis.ID, "variant " + SampleQcAnalysis.ID + "-run");

            put(TeamInterpretationAnalysis.ID, "clinical " + TeamInterpretationAnalysis.ID + "-run");
            put(TieringInterpretationAnalysis.ID, "clinical " + TieringInterpretationAnalysis.ID + "-run");
            put(ZettaInterpretationAnalysis.ID, "clinical " + ZettaInterpretationAnalysis.ID + "-run");
            put(CancerTieringInterpretationAnalysis.ID, "clinical " + CancerTieringInterpretationAnalysis.ID + "-run");

            put(JulieTool.ID, "variant julie-run");
        }};
    }

    public ExecutionDaemon(int interval, String token, CatalogManager catalogManager, String appHome) throws CatalogDBException {
        super(interval, token, catalogManager);

        this.jobManager = catalogManager.getJobManager();
        this.fileManager = catalogManager.getFileManager();
        this.internalCli = appHome + "/bin/opencga-internal.sh";

        this.defaultJobDir = Paths.get(catalogManager.getConfiguration().getJobDir());

        pendingJobsQuery = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
        queuedJobsQuery = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED);
        runningJobsQuery = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.RUNNING);
        // Sort jobs by priority and creation date
        queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, Arrays.asList(JobDBAdaptor.QueryParams.PRIORITY.key(),
                        JobDBAdaptor.QueryParams.CREATION_DATE.key()))
                .append(QueryOptions.ORDER, QueryOptions.ASCENDING);
    }

    @Override
    public void run() {
        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }

            try {
                checkJobs();
            } catch (Exception e) {
                logger.error("Catch exception " + e.getMessage(), e);
            }
        }

        try {
            logger.info("Attempt to shutdown webhook executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Webhook tasks interrupted");
        } finally {
            if (!executor.isTerminated()) {
                logger.error("Cancel non-finished webhook tasks");
            }
            executor.shutdownNow();
            logger.info("Webhook tasks finished");
        }
    }

    protected void checkJobs() {
        long pendingJobs = -1;
        long queuedJobs = -1;
        long runningJobs = -1;
        try {
            pendingJobs = jobManager.count(pendingJobsQuery, token).getNumMatches();
            queuedJobs = jobManager.count(queuedJobsQuery, token).getNumMatches();
            runningJobs = jobManager.count(runningJobsQuery, token).getNumMatches();
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
        logger.info("----- EXECUTION DAEMON  ----- pending={}, queued={}, running={}", pendingJobs, queuedJobs, runningJobs);

            /*
            PENDING JOBS
             */
        checkPendingJobs();

            /*
            QUEUED JOBS
             */
        checkQueuedJobs();

            /*
            RUNNING JOBS
             */
        checkRunningJobs();
    }

    protected void checkRunningJobs() {
        int handledRunningJobs = 0;
        try (DBIterator<Job> iterator = jobManager.iterator(runningJobsQuery, queryOptions, token)) {
            while (handledRunningJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                try {
                    Job job = iterator.next();
                    handledRunningJobs += checkRunningJob(job);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    protected int checkRunningJob(Job job) {
        Enums.ExecutionStatus jobStatus = getCurrentStatus(job);

        switch (jobStatus.getName()) {
            case Enums.ExecutionStatus.RUNNING:
                ExecutionResult result = readExecutionResult(job);
                if (result != null) {
                    if (result.getExecutor() != null
                            && result.getExecutor().getParams() != null
                            && result.getExecutor().getParams().containsKey(ParamConstants.TOKEN)) {
                        result.getExecutor().getParams().put(ParamConstants.TOKEN, REDACTED_TOKEN);
                    }
                    // Update the result of the job
                    PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams().setExecution(result);
                    try {
                        jobManager.update(job.getStudy().getId(), job.getId(), updateParams, QueryOptions.empty(), token);
                    } catch (CatalogException e) {
                        logger.error("[{}] - Could not update result information: {}", job.getId(), e.getMessage(), e);
                        return 0;
                    }
                }
                return 1;
            case Enums.ExecutionStatus.ABORTED:
            case Enums.ExecutionStatus.ERROR:
            case Enums.ExecutionStatus.DONE:
            case Enums.ExecutionStatus.READY:
                // Register job results
                return processFinishedJob(job, jobStatus);
            case Enums.ExecutionStatus.QUEUED:
                // Running job went back to Queued?
                logger.info("Running job '{}' went back to '{}' status", job.getId(), jobStatus.getName());
                return setStatus(job, new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED));
            case Enums.ExecutionStatus.PENDING:
            case Enums.ExecutionStatus.UNKNOWN:
            default:
                logger.info("Unexpected status '{}' for job '{}'", jobStatus.getName(), job.getId());
                return 0;

        }
    }

    protected void checkQueuedJobs() {
        int handledQueuedJobs = 0;
        try (DBIterator<Job> iterator = jobManager.iterator(queuedJobsQuery, queryOptions, token)) {
            while (handledQueuedJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                try {
                    Job job = iterator.next();
                    handledQueuedJobs += checkQueuedJob(job);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check if the job is still queued or it has changed to running or error.
     *
     * @param job Job object.
     * @return 1 if the job has changed the status, 0 otherwise.
     */
    protected int checkQueuedJob(Job job) {
        Enums.ExecutionStatus status = getCurrentStatus(job);

        switch (status.getName()) {
            case Enums.ExecutionStatus.QUEUED:
                // Job is still queued
                return 0;
            case Enums.ExecutionStatus.RUNNING:
                logger.info("[{}] - Updating status from {} to {}", job.getId(),
                        Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.RUNNING);
                logger.info("[{}] - stdout file '{}'", job.getId(),
                        job.getOutDir().getUri().resolve(getLogFileName(job)).getPath());
                logger.info("[{}] - stderr file: '{}'", job.getId(),
                        job.getOutDir().getUri().resolve(getErrorLogFileName(job)).getPath());
                return setStatus(job, new Enums.ExecutionStatus(Enums.ExecutionStatus.RUNNING));
            case Enums.ExecutionStatus.ABORTED:
            case Enums.ExecutionStatus.ERROR:
            case Enums.ExecutionStatus.DONE:
            case Enums.ExecutionStatus.READY:
                // Job has finished the execution, so we need to register the job results
                return processFinishedJob(job, status);
            case Enums.ExecutionStatus.UNKNOWN:
                logger.info("Job '{}' in status {}", job.getId(), Enums.ExecutionStatus.UNKNOWN);
                return 0;
            default:
                logger.info("Unexpected status '{}' for job '{}'", status.getName(), job.getId());
                return 0;
        }
    }

    protected void checkPendingJobs() {
        // Clear job counts each cycle
        jobsCountByType.clear();

        int handledPendingJobs = 0;
        try (DBIterator<Job> iterator = jobManager.iterator(pendingJobsQuery, queryOptions, token)) {
            while (handledPendingJobs < NUM_JOBS_HANDLED && iterator.hasNext()) {
                try {
                    Job job = iterator.next();
                    handledPendingJobs += checkPendingJob(job);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check everything is correct and queues the job.
     *
     * @param job Job object.
     * @return 1 if the job has changed the status, 0 otherwise.
     */
    protected int checkPendingJob(Job job) {
        if (StringUtils.isEmpty(job.getStudy().getId())) {
            return abortJob(job, "Missing mandatory 'studyUuid' field");
        }

        if (StringUtils.isEmpty(job.getTool().getId())) {
            return abortJob(job, "Tool id '" + job.getTool().getId() + "' not found.");
        }

        if (!canBeQueued(job)) {
            return 0;
        }

        Tool tool;
        try {
            tool = new ToolFactory().getTool(job.getTool().getId());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return abortJob(job, "Tool " + job.getTool().getId() + " not found");
        }

        try {
            checkToolExecutionPermission(job);
        } catch (Exception e) {
            return abortJob(job, e);
        }

        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams();
        updateParams.setTool(new ToolInfo(tool.id(), tool.description(), tool.scope(), tool.type(), tool.resource()));

        if (tool.scope() == Tool.Scope.PROJECT) {
            String projectFqn = job.getStudy().getId().substring(0, job.getStudy().getId().indexOf(ParamConstants.PROJECT_STUDY_SEPARATOR));
            try {
                List<String> studyFqnSet = catalogManager.getStudyManager().search(projectFqn, new Query(),
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.GROUPS.key(),
                                StudyDBAdaptor.QueryParams.FQN.key())), token)
                        .getResults()
                        .stream()
                        .map(Study::getFqn)
                        .filter(fqn -> !fqn.equals(job.getStudy().getId()))
                        .distinct()
                        .collect(Collectors.toList());

                updateParams.setStudy(new JobStudyParam(job.getStudy().getId(), studyFqnSet));
            } catch (CatalogException e) {
                return abortJob(job, e);
            }
        }

        String userToken;
        try {
            userToken = catalogManager.getUserManager().getNonExpiringToken(job.getUserId(), token);
        } catch (CatalogException e) {
            logger.error(e.getMessage(), e);
            return abortJob(job, "Internal error. Could not obtain token for user '" + job.getUserId() + "'");
        }

        if (CollectionUtils.isNotEmpty(job.getDependsOn())) {
            // The job(s) it depended on finished successfully. Check if the input files are correct.
            try {
                List<File> inputFiles = catalogManager.getJobManager().getJobInputFilesFromParams(job.getStudy().getId(), job, token);
                updateParams.setInput(inputFiles);
            } catch (CatalogException e) {
                return abortJob(job, e);
            }
        }


        Map<String, Object> params = job.getParams();
        String outDirPathParam = (String) params.get(OUTDIR_PARAM);
        if (!StringUtils.isEmpty(outDirPathParam)) {
            try {
                // Any path the user has requested
                updateParams.setOutDir(getValidInternalOutDir(job.getStudy().getId(), job, outDirPathParam, userToken));
            } catch (CatalogException e) {
                logger.error("Cannot create output directory. {}", e.getMessage(), e);
                return abortJob(job, "Cannot create output directory. " + e.getMessage());
            }
        } else {
            try {
                // JOBS/user/job_id/
                updateParams.setOutDir(getValidDefaultOutDir(job));
            } catch (CatalogException e) {
                logger.error("Cannot create output directory. {}", e.getMessage(), e);
                return abortJob(job, "Cannot create output directory. " + e.getMessage());
            }
        }

        Path outDirPath = Paths.get(updateParams.getOutDir().getUri());
        params.put(OUTDIR_PARAM, outDirPath.toAbsolutePath().toString());
        params.put(JOB_PARAM, job.getId());

        // Define where the stdout and stderr will be stored
        Path stderr = outDirPath.resolve(getErrorLogFileName(job));
        Path stdout = outDirPath.resolve(getLogFileName(job));

        // Create cli
        String commandLine = buildCli(internalCli, job);
        String authenticatedCommandLine = commandLine + " --token " + userToken;
        String shadedCommandLine = commandLine + " --token " + REDACTED_TOKEN;

        updateParams.setCommandLine(shadedCommandLine);

        logger.info("Updating job {} from {} to {}", job.getId(), Enums.ExecutionStatus.PENDING, Enums.ExecutionStatus.QUEUED);
        updateParams.setInternal(new JobInternal(new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED)));
        try {
            jobManager.update(job.getStudy().getId(), job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("Could not update job {}. {}", job.getId(), e.getMessage(), e);
            return 0;
        }

        try {
            String queue = getQueue(tool);
            logger.info("Queue job '{}' on queue '{}'", job.getId(), queue);
            batchExecutor.execute(job.getId(), queue, authenticatedCommandLine, stdout, stderr);
        } catch (Exception e) {
            logger.error("Error executing job {}.", job.getId(), e);
            return abortJob(job, "Error executing job. " + e.getMessage());
        }

        job.getInternal().setStatus(updateParams.getInternal().getStatus());
        notifyStatusChange(job);

        return 1;
    }

    protected void checkToolExecutionPermission(Job job) throws Exception {
        Tool tool = new ToolFactory().getTool(job.getTool().getId());

        if (tool.scope().equals(Tool.Scope.GLOBAL)) {
            if (!job.getUserId().equals(ParamConstants.OPENCGA_USER_ID)) {
                throw new CatalogAuthorizationException("Only user '" + ParamConstants.OPENCGA_USER_ID + "' "
                        + "can run tools with scope '" + Tool.Scope.GLOBAL + "'");
            }
        } else {
            if (job.getStudy().getId().startsWith(job.getUserId() + ParamConstants.USER_PROJECT_SEPARATOR)) {
                // If the user is the owner of the project, accept all.
                return;
            }

            // Validate user is owner or belongs to the right group
            String requiredGroup;
            if (tool.type().equals(Tool.Type.OPERATION)) {
                requiredGroup = ParamConstants.ADMINS_GROUP;
            } else {
                requiredGroup = ParamConstants.MEMBERS_GROUP;
            }

            List<Study> studiesToValidate;
            if (tool.scope() == Tool.Scope.PROJECT) {
                String projectFqn = job.getStudy().getId().substring(0, job.getStudy().getId().indexOf(ParamConstants.PROJECT_STUDY_SEPARATOR));
                studiesToValidate = catalogManager.getStudyManager().search(projectFqn, new Query(),
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.GROUPS.key(),
                                StudyDBAdaptor.QueryParams.FQN.key())), token).getResults();
            } else {
                studiesToValidate = catalogManager.getStudyManager().get(job.getStudy().getId(),
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.GROUPS.key(),
                                StudyDBAdaptor.QueryParams.FQN.key())), token).getResults();
            }

            List<String> missingStudies = new LinkedList<>();
            // It is not the owner, so we check if it is the right group
            for (Study study : studiesToValidate) {
                for (Group group : study.getGroups()) {
                    if (group.getId().equals(requiredGroup)) {
                        // If the user does not belong to the admins group
                        if (!group.getUserIds().contains(job.getUserId())) {
                            missingStudies.add(study.getFqn());
                        }
                        break;
                    }
                }
            }

            if (!missingStudies.isEmpty()) {
                throw new CatalogAuthorizationException("User '" + job.getUserId() + "' is not member of "
                        + requiredGroup + " of studies '" + missingStudies
                        + "'. The tool '" + job.getTool().getId()
                        + "' can only be executed by the project owners or members of " + requiredGroup);
            }

        }

    }

    private String getQueue(Tool tool) {
        String queue = "default";
        Execution execution = catalogManager.getConfiguration().getAnalysis().getExecution();
        if (StringUtils.isNotEmpty(execution.getDefaultQueue())) {
            queue = execution.getDefaultQueue();
        }
        if (execution.getToolsPerQueue() != null) {
            for (Map.Entry<String, List<String>> entry : execution.getToolsPerQueue().entrySet()) {
                if (entry.getValue().contains(tool.id())) {
                    queue = entry.getKey();
                }
            }
        }
        return queue;
    }

    private File getValidInternalOutDir(String study, Job job, String outDirPath, String userToken) throws CatalogException {
        // TODO: Remove this line when we stop passing the outdir as a query param in the URL
        outDirPath = outDirPath.replace(":", "/");
        if (!outDirPath.endsWith("/")) {
            outDirPath += "/";
        }
        File outDir;
        try {
            outDir = fileManager.get(study, outDirPath, FileManager.INCLUDE_FILE_URI_PATH, token).first();
        } catch (CatalogException e) {
            // Directory not found. Will try to create using user's token
            boolean parents = (boolean) job.getAttributes().getOrDefault(Job.OPENCGA_PARENTS, false);
            try {
                outDir = fileManager.createFolder(study, outDirPath, parents, "", FileManager.INCLUDE_FILE_URI_PATH,
                        userToken).first();
                IOManager ioManager = catalogManager.getIoManagerFactory().get(outDir.getUri());
                ioManager.createDirectory(outDir.getUri(), true);
            } catch (CatalogException | IOException e1) {
                throw new CatalogException("Cannot create output directory. " + e1.getMessage(), e1.getCause());
            }
        }

        // Ensure the directory is empty
        IOManager ioManager;
        try {
            ioManager = catalogManager.getIoManagerFactory().get(outDir.getUri());
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(outDir.getUri(), e);
        }
        if (!ioManager.isDirectory(outDir.getUri())) {
            throw new CatalogException(OUTDIR_PARAM + " seems not to be a directory");
        }
        if (!ioManager.listFiles(outDir.getUri()).isEmpty()) {
            throw new CatalogException(OUTDIR_PARAM + " " + outDirPath + " is not an empty directory");
        }

        return outDir;
    }

    private File getValidDefaultOutDir(Job job) throws CatalogException {
        File folder = fileManager.createFolder(job.getStudy().getId(), "JOBS/" + job.getUserId() + "/" + TimeUtils.getDay() + "/"
                        + job.getId(), true, "Job " + job.getTool().getId(), job.getId(), QueryOptions.empty(), token).first();

        // By default, OpenCGA will not create the physical folders until there is a file, so we need to create it manually
        try {
            catalogManager.getIoManagerFactory().get(folder.getUri()).createDirectory(folder.getUri(), true);
        } catch (CatalogIOException | IOException e) {
            // Submit job to delete job folder
            ObjectMap params = new ObjectMap()
                    .append("files", folder.getUuid())
                    .append("study", job.getStudy().getId())
                    .append(Constants.SKIP_TRASH, true);
            jobManager.submit(job.getStudy().getId(), FileDeleteTask.ID, Enums.Priority.LOW, params, token);
            throw new CatalogException("Cannot create job directory '" + folder.getUri() + "' for path '" + folder.getPath() + "'");
        }

        // Check if the user already has permissions set in his folder
        OpenCGAResult<Map<String, List<String>>> result = fileManager.getAcls(job.getStudy().getId(),
                Collections.singletonList("JOBS/" + job.getUserId() + "/"), job.getUserId(), true, token);
        if (result.getNumResults() == 0 || result.first().isEmpty() || ListUtils.isEmpty(result.first().get(job.getUserId()))) {
            // Add permissions to do anything under that path to the user launching the job
            String allFilePermissions = EnumSet.allOf(FileAclEntry.FilePermissions.class)
                    .stream()
                    .map(FileAclEntry.FilePermissions::toString)
                    .collect(Collectors.joining(","));
            fileManager.updateAcl(job.getStudy().getId(), Collections.singletonList("JOBS/" + job.getUserId() + "/"), job.getUserId(),
                    new FileAclParams(null, allFilePermissions), SET, token);
            // Remove permissions to the @members group
            fileManager.updateAcl(job.getStudy().getId(), Collections.singletonList("JOBS/" + job.getUserId() + "/"),
                    StudyManager.MEMBERS, new FileAclParams(null, ""), SET, token);
        }

        return folder;
    }

    public static String buildCli(String internalCli, Job job) {
        String toolId = job.getTool().getId();
        String internalCommand = TOOL_CLI_MAP.get(toolId);
        if (StringUtils.isEmpty(internalCommand)) {
            ObjectMap params = new ObjectMap()
                    .append(JOB_PARAM, job.getId())
                    .append(STUDY_PARAM, job.getStudy().getId());
            return buildCli(internalCli, "tools execute-job", params);
        } else {
            return buildCli(internalCli, internalCommand, job.getParams());
        }
    }

    public static String buildCli(String internalCli, String internalCommand, Map<String, Object> params) {
        StringBuilder cliBuilder = new StringBuilder()
                .append(internalCli)
                .append(" ").append(internalCommand);
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String key = entry.getKey();
            String param = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, key);
            if (entry.getValue() instanceof Map) {
                Map<String, String> dynamicParams = (Map<String, String>) entry.getValue();
                for (Map.Entry<String, String> dynamicEntry : dynamicParams.entrySet()) {
                    cliBuilder.append(" ").append("--").append(param).append(" ");
                    escapeCliArg(cliBuilder, dynamicEntry.getKey());
                    cliBuilder.append("=");
                    escapeCliArg(cliBuilder, dynamicEntry.getValue());
                }
            } else {
                if (!StringUtils.isAlphanumeric(StringUtils.replaceChars(key, "-_", ""))) {
                    // This should never happen
                    throw new IllegalArgumentException("Invalid job param key '" + key + "'");
                }
                cliBuilder
                        .append(" --").append(param)
                        .append(" ");
                escapeCliArg(cliBuilder, entry.getValue().toString());
            }
        }
        return cliBuilder.toString();
    }

    /**
     * Escape args if needed.
     *
     * Surround with single quotes. ('value')
     * Detect if the value had any single quote, and escape them with double quotes ("'")
     *
     *   --description It's true
     *   --description 'It'"'"'s true'
     *
     * 'It'
     * "'"
     * 's true'
     *
     * @param cliBuilder CommandLine StringBuilder
     * @param value value to escape
     */
    public static void escapeCliArg(StringBuilder cliBuilder, String value) {
        if (StringUtils.isAlphanumeric(value) || StringUtils.isEmpty(value)) {
            cliBuilder.append(value);
        } else {
            if (value.contains("'")) {
                value = value.replace("'", "'\"'\"'");
            }
            cliBuilder.append("'").append(value).append("'");
        }
    }

    private boolean canBeQueued(Job job) {
        if (job.getDependsOn() != null && !job.getDependsOn().isEmpty()) {
            for (Job tmpJob : job.getDependsOn()) {
                if (!Enums.ExecutionStatus.DONE.equals(tmpJob.getInternal().getStatus().getName())) {
                    if (Enums.ExecutionStatus.ABORTED.equals(tmpJob.getInternal().getStatus().getName())
                            || Enums.ExecutionStatus.ERROR.equals(tmpJob.getInternal().getStatus().getName())) {
                        abortJob(job, "Job '" + tmpJob.getId() + "' it depended on did not finish successfully");
                    }
                    return false;
                }
            }
        }

        if (!batchExecutor.canBeQueued()) {
            return false;
        }

        Integer maxJobs = catalogManager.getConfiguration().getAnalysis().getExecution().getMaxConcurrentJobs().get(job.getTool().getId());
        if (maxJobs == null) {
            // No limit for this tool
            return true;
        } else {
            return canBeQueued(job.getTool().getId(), maxJobs);
        }
    }

    private boolean canBeQueued(String toolId, int maxJobs) {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED + ","
                        + Enums.ExecutionStatus.RUNNING)
                .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), toolId);
        long currentJobs = jobsCountByType.computeIfAbsent(toolId, k -> {
            try {
                return catalogManager.getJobManager().count(query, token).getNumMatches();
            } catch (CatalogException e) {
                logger.error("Error counting the current number of running and queued \"" + toolId + "\" jobs", e);
                return 0L;
            }
        });
        if (currentJobs >= maxJobs) {
            long now = System.currentTimeMillis();
            Long lastTimeLog = retainedLogsTime.getOrDefault(toolId, 0L);
            if (now - lastTimeLog > 60000) {
                logger.info("There are {} " + toolId + " jobs running or queued already. "
                        + "Current limit is {}. "
                        + "Halt new " + toolId + " jobs.", currentJobs, maxJobs);
                retainedLogsTime.put(toolId, now);
            }
            return false;
        } else {
            jobsCountByType.remove(toolId);
            retainedLogsTime.put(toolId, 0L);
            return true;
        }
    }

    private int abortJob(Job job, Exception e) {
        logger.error(e.getMessage(), e);
        return abortJob(job, e.getMessage());
    }

    private int abortJob(Job job, String description) {
        logger.info("Aborting job: {} - Reason: '{}'", job.getId(), description);
        return setStatus(job, new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED, description));
    }

    private int setStatus(Job job, Enums.ExecutionStatus status) {
        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams().setInternal(new JobInternal(status));

        try {
            jobManager.update(job.getStudy().getId(), job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("Unexpected error. Cannot update job '{}' to status '{}'. {}", job.getId(),
                    updateParams.getInternal().getStatus().getName(), e.getMessage(), e);
            return 0;
        }

        job.getInternal().setStatus(status);
        notifyStatusChange(job);

        return 1;
    }

    private Enums.ExecutionStatus getCurrentStatus(Job job) {

        Path resultJson = getExecutionResultPath(job);

        // Check if analysis result file is there
        if (resultJson != null && Files.exists(resultJson)) {
            ExecutionResult execution = readExecutionResult(resultJson);
            if (execution != null) {
                Instant lastStatusUpdate = execution.getStatus().getDate().toInstant();
                if (lastStatusUpdate.until(Instant.now(), ChronoUnit.MINUTES) > EXECUTION_RESULT_FILE_EXPIRATION_MINUTES) {
                    logger.warn("Ignoring file '" + resultJson + "'. The file is more than "
                            + EXECUTION_RESULT_FILE_EXPIRATION_MINUTES + " minutes old");
                } else {
                    return new Enums.ExecutionStatus(execution.getStatus().getName().name());
                }
            } else {
                if (Files.exists(resultJson)) {
                    logger.warn("File '" + resultJson + "' seems corrupted.");
                } else {
                    logger.warn("Could not find file '" + resultJson + "'.");
                }
            }
        }

        String status = batchExecutor.getStatus(job.getId());
        if (!StringUtils.isEmpty(status) && !status.equals(Enums.ExecutionStatus.UNKNOWN)) {
            return new Enums.ExecutionStatus(status);
        } else {
            Path tmpOutdirPath = Paths.get(job.getOutDir().getUri());
            // Check if the error file is present
            Path errorLog = tmpOutdirPath.resolve(getErrorLogFileName(job));

            if (Files.exists(errorLog)) {
                // FIXME: This may not be true. There is a delay between job starts (i.e. error log appears) and
                //  the analysis result creation

                // There must be some command line error. The job started running but did not finish well, otherwise we would find the
                // analysis-result.yml file
                return new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Command line error");
            } else {
                return new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED);
            }
        }

    }

    private Path getExecutionResultPath(Job job) {
        Path resultJson = null;
        try (Stream<Path> stream = Files.list(Paths.get(job.getOutDir().getUri()))) {
            resultJson = stream
                    .filter(path -> {
                        String str = path.toString();
                        return str.endsWith(ExecutionResultManager.FILE_EXTENSION)
                                && !str.endsWith(ExecutionResultManager.SWAP_FILE_EXTENSION);
                    })
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            logger.warn("Could not find AnalysisResult file", e);
        }
        return resultJson;
    }

    private ExecutionResult readExecutionResult(Job job) {
        Path resultJson = getExecutionResultPath(job);
        if (resultJson != null) {
            return readExecutionResult(resultJson);
        }
        return null;
    }

    private ExecutionResult readExecutionResult(Path file) {
        if (file == null) {
            return null;
        }
        int attempts = 0;
        int maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                try (InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()))) {
                    return JacksonUtils.getDefaultObjectMapper().readValue(is, ExecutionResult.class);
                }
            } catch (IOException e) {
                if (attempts == maxAttempts) {
                    logger.error("Could not load AnalysisResult file: " + file.toAbsolutePath(), e);
                } else {
                    logger.warn("Could not load AnalysisResult file: " + file.toAbsolutePath()
                            + ". Retry " + attempts + "/" + maxAttempts
                            + ". " + e.getMessage()
                    );
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException interruption) {
                        // Ignore interruption
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        return null;
    }

    private int processFinishedJob(Job job, Enums.ExecutionStatus status) {
        logger.info("[{}] - Processing finished job with status {}", job.getId(), status.getName());

        Path outDirUri = Paths.get(job.getOutDir().getUri());
        Path analysisResultPath = getExecutionResultPath(job);

        logger.info("[{}] - Registering job results from '{}'", job.getId(), outDirUri);

        ExecutionResult execution;
        if (analysisResultPath != null) {
            execution = readExecutionResult(analysisResultPath);
            if (execution != null) {
                PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams().setExecution(execution);
                try {
                    jobManager.update(job.getStudy().getId(), job.getId(), updateParams, QueryOptions.empty(), token);
                } catch (CatalogException e) {
                    logger.error("[{}] - Catastrophic error. Could not update job information with final result {}: {}", job.getId(),
                            updateParams.toString(), e.getMessage(), e);
                    return 0;
                }
            }
        } else {
            execution = null;
        }

        List<File> registeredFiles;
        try {
            Predicate<URI> uriPredicate = uri -> !uri.getPath().endsWith(ExecutionResultManager.FILE_EXTENSION)
                    && !uri.getPath().endsWith(ExecutionResultManager.SWAP_FILE_EXTENSION)
                    && !uri.getPath().contains("/scratch_");
            registeredFiles = fileManager.syncUntrackedFiles(job.getStudy().getId(), job.getOutDir().getPath(), uriPredicate, job.getId(),
                    token).getResults();
        } catch (CatalogException e) {
            logger.error("Could not registered files in Catalog: {}", e.getMessage(), e);
            return 0;
        }

        // Register the job information
        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams();

        // Process output and log files
        List<File> outputFiles = new LinkedList<>();
        String logFileName = getLogFileName(job);
        String errorLogFileName = getErrorLogFileName(job);
        for (File registeredFile : registeredFiles) {
            if (registeredFile.getName().equals(logFileName)) {
                logger.info("[{}] - stdout file '{}'", job.getId(), registeredFile.getUri().getPath());
                updateParams.setStdout(registeredFile);
            } else if (registeredFile.getName().equals(errorLogFileName)) {
                logger.info("[{}] - stderr file: '{}'", job.getId(), registeredFile.getUri().getPath());
                updateParams.setStderr(registeredFile);
            } else {
                outputFiles.add(registeredFile);
            }
        }
        if (execution != null) {
            for (URI externalFile : execution.getExternalFiles()) {
                Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), externalFile);
                try {
                    OpenCGAResult<File> search = fileManager.search(job.getStudy().getId(), query, FileManager.INCLUDE_FILE_URI_PATH,
                            token);
                    if (search.getNumResults() == 0) {
                        throw new CatalogException("File not found");
                    }
                    outputFiles.add(search.first());
                } catch (CatalogException e) {
                    logger.error("Could not obtain external file {}: {}", externalFile, e.getMessage(), e);
                    return 0;
                }
            }
        }
        updateParams.setOutput(outputFiles);


        updateParams.setInternal(new JobInternal());
        // Check status of analysis result or if there are files that could not be moved to outdir to decide the final result
        if (execution == null) {
            updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR,
                    "Job could not finish successfully. Missing execution result"));
        } else if (execution.getStatus().getName().equals(Status.Type.ERROR)) {
            updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR,
                    "Job could not finish successfully"));
        } else {
            switch (status.getName()) {
                case Enums.ExecutionStatus.DONE:
                case Enums.ExecutionStatus.READY:
                    updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE));
                    break;
                case Enums.ExecutionStatus.ABORTED:
                    updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Job aborted!"));
                    break;
                case Enums.ExecutionStatus.ERROR:
                default:
                    updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR,
                            "Job could not finish successfully"));
                    break;
            }
        }

        logger.info("[{}] - Updating job information", job.getId());
        // We update the job information
        try {
            jobManager.update(job.getStudy().getId(), job.getId(), updateParams, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            logger.error("[{}] - Catastrophic error. Could not update job information with final result {}: {}", job.getId(),
                    updateParams.toString(), e.getMessage(), e);
            return 0;
        }

        job.getInternal().setStatus(updateParams.getInternal().getStatus());
        notifyStatusChange(job);

        return 1;
    }

    private void notifyStatusChange(Job job) {
        if (job.getInternal().getWebhook().getUrl() != null) {
            executor.submit(() -> {
                try {
                    sendWebhookNotification(job, job.getInternal().getWebhook().getUrl());
                } catch (URISyntaxException | CatalogException | CloneNotSupportedException e) {
                    logger.warn("Could not store notification status: {}", e.getMessage(), e);
                }
            });
        }
    }

    private void sendWebhookNotification(Job job, URL url) throws URISyntaxException, CatalogException, CloneNotSupportedException {
        JobInternal jobInternal = new JobInternal(null, job.getInternal().getWebhook().clone(), null);
        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams()
                .setInternal(jobInternal);

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(JobDBAdaptor.QueryParams.INTERNAL_EVENTS.key(), ParamUtils.BasicUpdateAction.ADD.name());
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        Client client = ClientBuilder.newClient();
        Response post;
        try {
            post = client
                    .target(url.toURI())
                    .request(MediaType.APPLICATION_JSON)
                    .property(ClientProperties.CONNECT_TIMEOUT, 1000)
                    .property(ClientProperties.READ_TIMEOUT, 5000)
                    .post(Entity.json(job));
        } catch (ProcessingException e) {
            jobInternal.getWebhook().getStatus().put(job.getInternal().getStatus().getName(), JobInternalWebhook.Status.ERROR);
            jobInternal.setEvents(Collections.singletonList(new Event(Event.Type.ERROR, "Could not notify through webhook. "
                    + e.getMessage())));

            jobManager.update(job.getStudy().getId(), job.getId(), updateParams, options, token);

            return;
        }
        if (post.getStatus() == HttpStatus.SC_OK) {
            jobInternal.getWebhook().getStatus().put(job.getInternal().getStatus().getName(), JobInternalWebhook.Status.SUCCESS);
        } else {
            jobInternal.getWebhook().getStatus().put(job.getInternal().getStatus().getName(), JobInternalWebhook.Status.ERROR);
            jobInternal.setEvents(Collections.singletonList(new Event(Event.Type.ERROR, "Could not notify through webhook. HTTP response "
                    + "code: " + post.getStatus())));
        }

        jobManager.update(job.getStudy().getId(), job.getId(), updateParams, options, token);
    }

    private String getErrorLogFileName(Job job) {
        return job.getId() + ".err";
    }

    private String getLogFileName(Job job) {
        // WARNING: If we change the way we name log files, we will also need to change it in the "log" method from the JobManager !!
        return job.getId() + ".log";
    }

}
