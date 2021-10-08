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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.rga.AuxiliarRgaAnalysis;
import org.opencb.opencga.analysis.clinical.rga.RgaAnalysis;
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
import org.opencb.opencga.analysis.templates.TemplateRunner;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
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
import org.opencb.opencga.analysis.wrappers.bwa.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.gatk.GatkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.picard.PicardWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.plink.PlinkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.rvtests.RvtestsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ExecutionManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by imedina on 16/06/16.
 */
public class ExecutionDaemon extends MonitorParentDaemon {

    public static final String OUTDIR_PARAM = "outdir";
    public static final int EXECUTION_RESULT_FILE_EXPIRATION_MINUTES = 10;
    public static final String REDACTED_TOKEN = "xxxxxxxxxxxxxxxxxxxxx";
    private final String internalCli;
    private final ExecutionManager executionManager;
    private final FileManager fileManager;
    private final Map<String, Long> executionsCountByType = new HashMap<>();
    private final Map<String, Long> retainedLogsTime = new HashMap<>();

    private Path defaultExecutionDir;

    private static final Map<String, String> TOOL_CLI_MAP;

    // Maximum number of executions of each type (Pending, queued, running) that will be handled on each iteration.
    // Example: If there are 100 pending executions, 15 queued, 70 running.
    // On first iteration, it will queue 50 out of the 100 pending executions. It will check up to 50 queue-running changes out of the 65
    // (15 + 50 from pending), and it will check up to 50 finished executions from the running ones.
    // On second iteration, it will queue the remaining 50 pending executions, and so on...
    private static final int NUM_HANDLED = 50;
    private final Query pendingExecutionsQuery;
    private final Query queuedExecutionsQuery;
    private final Query runningExecutionsQuery;

    private final QueryOptions queryOptions;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    static {
        TOOL_CLI_MAP = new HashMap<String, String>() {{
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

            put(RgaAnalysis.ID, "clinical " + RgaAnalysis.ID + "-run");
            put(AuxiliarRgaAnalysis.ID, "clinical " + AuxiliarRgaAnalysis.ID + "-run");

            put(JulieTool.ID, "variant julie-run");

            put(TemplateRunner.ID, "studies " + TemplateRunner.ID + "-run");
        }};
    }

    public ExecutionDaemon(int interval, String token, CatalogManager catalogManager, String appHome) throws CatalogDBException {
        super(interval, token, catalogManager);

        this.executionManager = catalogManager.getExecutionManager();
        this.fileManager = catalogManager.getFileManager();
        this.internalCli = appHome + "/bin/opencga-internal.sh";

        this.defaultExecutionDir = Paths.get(catalogManager.getConfiguration().getJobDir());

        pendingExecutionsQuery = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
        queuedExecutionsQuery = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED);
        runningExecutionsQuery = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.RUNNING);

        // Sort executions by priority and creation date
        queryOptions = new QueryOptions()
                .append(QueryOptions.SORT, Arrays.asList(ExecutionDBAdaptor.QueryParams.PRIORITY.key(),
                        ExecutionDBAdaptor.QueryParams.CREATION_DATE.key()))
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
                checkExecutions();
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

    protected void checkExecutions() {
        long pendingExecutions = -1;
        long queuedExecutions = -1;
        long runningExecutions = -1;
        try {
            pendingExecutions = executionManager.count(pendingExecutionsQuery, token).getNumMatches();
            queuedExecutions = executionManager.count(queuedExecutionsQuery, token).getNumMatches();
            runningExecutions = executionManager.count(runningExecutionsQuery, token).getNumMatches();
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
        logger.info("----- JOB DAEMON  ----- pending={}, queued={}, running={}", pendingExecutions, queuedExecutions, runningExecutions);

            /*
            PENDING JOBS
             */
        checkPendingExecutions();

            /*
            QUEUED JOBS
             */
        checkQueuedExecutions();

            /*
            RUNNING JOBS
             */
        checkRunningExecutions();
    }

    protected void checkRunningExecutions() {
        int handledRunningExecutions = 0;
        try (DBIterator<Execution> iterator = executionManager.iterator(runningExecutionsQuery, queryOptions, token)) {
            while (handledRunningExecutions < NUM_HANDLED && iterator.hasNext()) {
                try {
                    Execution execution = iterator.next();
                    handledRunningExecutions += checkRunningExecution(execution);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    protected int checkRunningExecution(Execution execution) {
        return 1;
//        Enums.ExecutionStatus executionStatus = getCurrentStatus(execution);
//
//        switch (executionStatus.getName()) {
//            case Enums.ExecutionStatus.RUNNING:
//                ExecutionResult result = readExecutionResult(execution);
//                if (result != null) {
//                    if (result.getExecutor() != null
//                            && result.getExecutor().getParams() != null
//                            && result.getExecutor().getParams().containsKey(ParamConstants.TOKEN)) {
//                        result.getExecutor().getParams().put(ParamConstants.TOKEN, REDACTED_TOKEN);
//                    }
//                    // Update the result of the execution
//                    PrivateExecutionUpdateParams updateParams = new PrivateExecutionUpdateParams().setExecution(result);
//                    try {
//                        executionManager.update(execution.getStudy().getId(), execution.getId(), updateParams, QueryOptions.empty(),
//                        token);
//                    } catch (CatalogException e) {
//                        logger.error("[{}] - Could not update result information: {}", execution.getId(), e.getMessage(), e);
//                        return 0;
//                    }
//                }
//                return 1;
//            case Enums.ExecutionStatus.ABORTED:
//            case Enums.ExecutionStatus.ERROR:
//            case Enums.ExecutionStatus.DONE:
//            case Enums.ExecutionStatus.READY:
//                // Register execution results
//                return processFinishedExecution(execution, executionStatus);
//            case Enums.ExecutionStatus.QUEUED:
//                // Running execution went back to Queued?
//                logger.info("Running execution '{}' went back to '{}' status", execution.getId(), executionStatus.getName());
//                return setStatus(execution, new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED));
//            case Enums.ExecutionStatus.PENDING:
//            case Enums.ExecutionStatus.UNKNOWN:
//            default:
//                logger.info("Unexpected status '{}' for execution '{}'", executionStatus.getName(), execution.getId());
//                return 0;
//
//        }
    }

    protected void checkQueuedExecutions() {
        int handledQueuedExecutions = 0;
        try (DBIterator<Execution> iterator = executionManager.iterator(queuedExecutionsQuery, queryOptions, token)) {
            while (handledQueuedExecutions < NUM_HANDLED && iterator.hasNext()) {
                try {
                    Execution execution = iterator.next();
                    handledQueuedExecutions += checkQueuedExecution(execution);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check if the execution is still queued or it has changed to running or error.
     *
     * @param execution Execution object.
     * @return 1 if the execution has changed the status, 0 otherwise.
     */
    protected int checkQueuedExecution(Execution execution) {
        return 0;
//        Enums.ExecutionStatus status = getCurrentStatus(execution);
//
//        switch (status.getName()) {
//            case Enums.ExecutionStatus.QUEUED:
//                // Execution is still queued
//                return 0;
//            case Enums.ExecutionStatus.RUNNING:
//                logger.info("[{}] - Updating status from {} to {}", execution.getId(),
//                        Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.RUNNING);
//                logger.info("[{}] - stdout file '{}'", execution.getId(),
//                        execution.getOutDir().getUri().resolve(getLogFileName(execution)).getPath());
//                logger.info("[{}] - stderr file: '{}'", execution.getId(),
//                        execution.getOutDir().getUri().resolve(getErrorLogFileName(execution)).getPath());
//                return setStatus(execution, new Enums.ExecutionStatus(Enums.ExecutionStatus.RUNNING));
//            case Enums.ExecutionStatus.ABORTED:
//            case Enums.ExecutionStatus.ERROR:
//            case Enums.ExecutionStatus.DONE:
//            case Enums.ExecutionStatus.READY:
//                // Execution has finished the execution, so we need to register the execution results
//                return processFinishedExecution(execution, status);
//            case Enums.ExecutionStatus.UNKNOWN:
//                logger.info("Execution '{}' in status {}", execution.getId(), Enums.ExecutionStatus.UNKNOWN);
//                return 0;
//            default:
//                logger.info("Unexpected status '{}' for execution '{}'", status.getName(), execution.getId());
//                return 0;
//        }
    }

    protected void checkPendingExecutions() {
        // Clear execution counts each cycle
        executionsCountByType.clear();

        int handledPendingExecutions = 0;
        try (DBIterator<Execution> iterator = executionManager.iterator(pendingExecutionsQuery, queryOptions, token)) {
            while (handledPendingExecutions < NUM_HANDLED && iterator.hasNext()) {
                try {
                    Execution execution = iterator.next();
                    handledPendingExecutions += checkPendingExecution(execution);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check everything is correct and queues the execution.
     *
     * @param execution Execution object.
     * @return 1 if the execution has changed the status, 0 otherwise.
     */
    protected int checkPendingExecution(Execution execution) {
        if (StringUtils.isEmpty(execution.getStudy().getId())) {
            return abortExecution(execution, "Missing mandatory 'studyUuid' field");
        }

        if (StringUtils.isEmpty(execution.getToolId())) {
            return abortExecution(execution, "Tool id '" + execution.getToolId() + "' not found.");
        }

        if (!canBeQueued(execution)) {
            return 0;
        }

        List<Job> jobList = new LinkedList<>();
        if (execution.isIsPipeline()) {
            Map<String, String> toolIdJobIdMap = new HashMap<>();
            for (Pipeline.JobDefinition jobDefinition : execution.getPipeline().getJobs()) {
                List<String> dependsOn = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(jobDefinition.getDependsOn())) {
                    for (String jobDefinitionId : jobDefinition.getDependsOn()) {
                        if (!toolIdJobIdMap.containsKey(jobDefinitionId)) {
                            return abortExecution(execution, "Job definition list is not properly ordered in the Pipeline");
                        }
                        dependsOn.add(toolIdJobIdMap.get(jobDefinitionId));
                    }
                }

                Map<String, Object> jobParams;
                try {
                    jobParams = getJobParams(execution.getParams(), jobDefinition.getParams(), execution.getPipeline().getParams(),
                            jobDefinition.getId());
                } catch (ToolException e) {
                    return abortExecution(execution, e.getMessage());
                }
                Job job = createJobInstance(execution.getId(), jobDefinition.getId(), jobDefinition.getDescription(),
                        execution.getPriority(), jobParams, execution.getTags(), dependsOn, execution.getUserId());
                jobList.add(job);

                toolIdJobIdMap.put(jobDefinition.getId(), job.getId());
            }
        } else {
            // Not pipeline
            Job job = createJobInstance(execution.getId(), execution.getToolId(), execution.getDescription(),
                    execution.getPriority(), execution.getParams(), execution.getTags(), Collections.emptyList(), execution.getUserId());
            jobList.add(job);
        }

        // TODO: Submit all jobs in single transaction

        // Update execution (new status and new list of jobs?)
        setStatus(execution,
                new Enums.ExecutionStatus(Enums.ExecutionStatus.PROCESSED, "Execution has been processed and all jobs created"));
        return 1;
    }

    private Job createJobInstance(String executionId, String toolId, String description, Enums.Priority priority,
                                  Map<String, Object> params, List<String> tags, List<String> dependsOn, String userId) {
        return new Job("", "", description, executionId, StringUtils.isNotEmpty(toolId) ? new ToolInfo().setId(toolId) : null, userId, null,
                params, null, null, priority, null, null, null, null,
                CollectionUtils.isNotEmpty(dependsOn)
                        ? dependsOn.stream().map(dpo -> new Job().setId(dpo)).collect(Collectors.toList())
                        : null,
                tags, null, false, null, null, 0, null, null);
    }

    private Map<String, Object> getJobParams(Map<String, Object> userParams, Map<String, Object> jobParams,
                                             Map<String, Object> pipelineParams, String toolId) throws ToolException {
        Map<String, Object> params = userParams != null ? new HashMap<>(userParams) : new HashMap<>();   // User params
        jobParams.forEach(params::putIfAbsent);                                                          // Merge with job params
        pipelineParams.forEach(params::putIfAbsent);                                                     // Merge with pipeline params

        // Extract all the allowed params for the job
        Class<? extends OpenCgaTool> toolClass = new ToolFactory().getToolClass(toolId);
        Set<String> jobAllowedParams = new HashSet<>();
        jobAllowedParams.add("study");
        for (Field declaredField : toolClass.getDeclaredFields()) {
            if (declaredField.getDeclaredAnnotations().length > 0) {
                if (declaredField.getDeclaredAnnotations()[0].annotationType().getName().equals(ToolParams.class.getName())) {
                    for (Field field : declaredField.getType().getDeclaredFields()) {
                        jobAllowedParams.add(field.getName());
                    }
                }
            }
        }

        if (jobAllowedParams.size() == 1) {
            throw new ToolException("Could not find a ToolParams annotation for the tool '" + toolId + "'");
        }

        // Remove all params that are not in the allowed params list
        for (String key : params.keySet()) {
            if (!jobAllowedParams.contains(key)) {
                params.remove(key);
            }
        }

        return params;
    }

//    protected void checkToolExecutionPermission(Execution execution) throws Exception {
//        Tool tool = new ToolFactory().getTool(execution.getTool().getId());
//
//        if (catalogManager.getAuthorizationManager().isInstallationAdministrator(execution.getUserId())) {
//            // Installation administrator user can run everything
//            return;
//        }
//        if (tool.scope().equals(Tool.Scope.GLOBAL)) {
//            throw new CatalogAuthorizationException("Only user '" + ParamConstants.OPENCGA_USER_ID + "' "
//                    + "can run tools with scope '" + Tool.Scope.GLOBAL + "'");
//        } else {
//            if (execution.getStudy().getId().startsWith(execution.getUserId() + ParamConstants.USER_PROJECT_SEPARATOR)) {
//                // If the user is the owner of the project, accept all.
//                return;
//            }
//
//            // Validate user is owner or belongs to the right group
//            String requiredGroup;
//            if (tool.type().equals(Tool.Type.OPERATION)) {
//                requiredGroup = ParamConstants.ADMINS_GROUP;
//            } else {
//                requiredGroup = ParamConstants.MEMBERS_GROUP;
//            }
//
//            List<Study> studiesToValidate;
//            if (tool.scope() == Tool.Scope.PROJECT) {
//                String projectFqn = execution.getStudy().getId()
//                        .substring(0, execution.getStudy().getId().indexOf(ParamConstants.PROJECT_STUDY_SEPARATOR));
//                studiesToValidate = catalogManager.getStudyManager().search(projectFqn, new Query(),
//                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.GROUPS.key(),
//                                StudyDBAdaptor.QueryParams.FQN.key())), token).getResults();
//            } else {
//                studiesToValidate = catalogManager.getStudyManager().get(execution.getStudy().getId(),
//                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.GROUPS.key(),
//                                StudyDBAdaptor.QueryParams.FQN.key())), token).getResults();
//            }
//
//            List<String> missingStudies = new LinkedList<>();
//            // It is not the owner, so we check if it is the right group
//            for (Study study : studiesToValidate) {
//                for (Group group : study.getGroups()) {
//                    if (group.getId().equals(requiredGroup)) {
//                        // If the user does not belong to the admins group
//                        if (!group.getUserIds().contains(execution.getUserId())) {
//                            missingStudies.add(study.getFqn());
//                        }
//                        break;
//                    }
//                }
//            }
//
//            if (!missingStudies.isEmpty()) {
//                throw new CatalogAuthorizationException("User '" + execution.getUserId() + "' is not member of "
//                        + requiredGroup + " of studies '" + missingStudies
//                        + "'. The tool '" + execution.getTool().getId()
//                        + "' can only be executed by the project owners or members of " + requiredGroup);
//            }
//
//        }
//
//    }

//    private String getQueue(Tool tool) {
//        String queue = "default";
//        Execution execution = catalogManager.getConfiguration().getAnalysis().getExecution();
//        if (StringUtils.isNotEmpty(execution.getDefaultQueue())) {
//            queue = execution.getDefaultQueue();
//        }
//        if (execution.getToolsPerQueue() != null) {
//            for (Map.Entry<String, List<String>> entry : execution.getToolsPerQueue().entrySet()) {
//                if (entry.getValue().contains(tool.id())) {
//                    queue = entry.getKey();
//                }
//            }
//        }
//        return queue;
//    }

//    private File getValidInternalOutDir(String study, Execution execution, String outDirPath, String userToken) throws CatalogException {
//        // TODO: Remove this line when we stop passing the outdir as a query param in the URL
//        outDirPath = outDirPath.replace(":", "/");
//        if (!outDirPath.endsWith("/")) {
//            outDirPath += "/";
//        }
//        File outDir;
//        try {
//            outDir = fileManager.get(study, outDirPath, FileManager.INCLUDE_FILE_URI_PATH, token).first();
//        } catch (CatalogException e) {
//            // Directory not found. Will try to create using user's token
//            boolean parents = (boolean) execution.getAttributes().getOrDefault(Execution.OPENCGA_PARENTS, false);
//            try {
//                outDir = fileManager.createFolder(study, outDirPath, parents, "", FileManager.INCLUDE_FILE_URI_PATH,
//                        userToken).first();
//                IOManager ioManager = catalogManager.getIoManagerFactory().get(outDir.getUri());
//                ioManager.createDirectory(outDir.getUri(), true);
//            } catch (CatalogException | IOException e1) {
//                throw new CatalogException("Cannot create output directory. " + e1.getMessage(), e1.getCause());
//            }
//        }
//
//        // Ensure the directory is empty
//        IOManager ioManager;
//        try {
//            ioManager = catalogManager.getIoManagerFactory().get(outDir.getUri());
//        } catch (IOException e) {
//            throw CatalogIOException.ioManagerException(outDir.getUri(), e);
//        }
//        if (!ioManager.isDirectory(outDir.getUri())) {
//            throw new CatalogException(OUTDIR_PARAM + " seems not to be a directory");
//        }
//        if (!ioManager.listFiles(outDir.getUri()).isEmpty()) {
//            throw new CatalogException(OUTDIR_PARAM + " " + outDirPath + " is not an empty directory");
//        }
//
//        return outDir;
//    }

//    private File getValidDefaultOutDir(Execution execution) throws CatalogException {
//        File folder = fileManager.createFolder(execution.getStudy().getId(), "JOBS/" + execution.getUserId() + "/" + TimeUtils.getDay()
//        + "/"
//                + execution.getId(), true, "Execution " + execution.getTool().getId(), execution.getId(), QueryOptions.empty(), token)
//                .first();
//
//        // By default, OpenCGA will not create the physical folders until there is a file, so we need to create it manually
//        try {
//            catalogManager.getIoManagerFactory().get(folder.getUri()).createDirectory(folder.getUri(), true);
//        } catch (CatalogIOException | IOException e) {
//            // Submit execution to delete execution folder
//            ObjectMap params = new ObjectMap()
//                    .append("files", folder.getUuid())
//                    .append("study", execution.getStudy().getId())
//                    .append(Constants.SKIP_TRASH, true);
//            executionManager.submit(execution.getStudy().getId(), FileDeleteTask.ID, Enums.Priority.LOW, params, token);
//            throw new CatalogException("Cannot create execution directory '" + folder.getUri() + "' for path '" + folder.getPath() + "'");
//        }
//
//        // Check if the user already has permissions set in his folder
//        OpenCGAResult<Map<String, List<String>>> result = fileManager.getAcls(execution.getStudy().getId(),
//                Collections.singletonList("JOBS/" + execution.getUserId() + "/"), execution.getUserId(), true, token);
//        if (result.getNumResults() == 0 || result.first().isEmpty() || ListUtils.isEmpty(result.first().get(execution.getUserId()))) {
//            // Add permissions to do anything under that path to the user launching the execution
//            String allFilePermissions = EnumSet.allOf(FileAclEntry.FilePermissions.class)
//                    .stream()
//                    .map(FileAclEntry.FilePermissions::toString)
//                    .collect(Collectors.joining(","));
//            fileManager.updateAcl(execution.getStudy().getId(), Collections.singletonList("JOBS/" + execution.getUserId() + "/"),
//            execution.getUserId(),
//                    new FileAclParams(null, allFilePermissions), SET, token);
//            // Remove permissions to the @members group
//            fileManager.updateAcl(execution.getStudy().getId(), Collections.singletonList("JOBS/" + execution.getUserId() + "/"),
//                    StudyManager.MEMBERS, new FileAclParams(null, ""), SET, token);
//        }
//
//        return folder;
//    }

    private boolean canBeQueued(Execution execution) {
        return true;
//        if (execution.getDependsOn() != null && !execution.getDependsOn().isEmpty()) {
//            for (Execution tmpExecution : execution.getDependsOn()) {
//                if (!Enums.ExecutionStatus.DONE.equals(tmpExecution.getInternal().getStatus().getName())) {
//                    if (Enums.ExecutionStatus.ABORTED.equals(tmpExecution.getInternal().getStatus().getName())
//                            || Enums.ExecutionStatus.ERROR.equals(tmpExecution.getInternal().getStatus().getName())) {
//                        abortExecution(execution, "Execution '" + tmpExecution.getId() + "' it depended on did not finish successfully");
//                    }
//                    return false;
//                }
//            }
//        }
//
//        if (!batchExecutor.canBeQueued()) {
//            return false;
//        }
//
//        Integer maxExecutions = catalogManager.getConfiguration().getAnalysis().getExecution().getMaxConcurrentExecutions()
//        .get(execution.getTool().getId());
//        if (maxExecutions == null) {
//            // No limit for this tool
//            return true;
//        } else {
//            return canBeQueued(execution.getTool().getId(), maxExecutions);
//        }
    }

//    private boolean canBeQueued(String toolId, int maxExecutions) {
//        Query query = new Query()
//                .append(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.QUEUED + ","
//                        + Enums.ExecutionStatus.RUNNING)
//                .append(ExecutionDBAdaptor.QueryParams.TOOL_ID.key(), toolId);
//        long currentExecutions = executionsCountByType.computeIfAbsent(toolId, k -> {
//            try {
//                return catalogManager.getExecutionManager().count(query, token).getNumMatches();
//            } catch (CatalogException e) {
//                logger.error("Error counting the current number of running and queued \"" + toolId + "\" executions", e);
//                return 0L;
//            }
//        });
//        if (currentExecutions >= maxExecutions) {
//            long now = System.currentTimeMillis();
//            Long lastTimeLog = retainedLogsTime.getOrDefault(toolId, 0L);
//            if (now - lastTimeLog > 60000) {
//                logger.info("There are {} " + toolId + " executions running or queued already. "
//                        + "Current limit is {}. "
//                        + "Halt new " + toolId + " executions.", currentExecutions, maxExecutions);
//                retainedLogsTime.put(toolId, now);
//            }
//            return false;
//        } else {
//            executionsCountByType.remove(toolId);
//            retainedLogsTime.put(toolId, 0L);
//            return true;
//        }
//    }
//
//    private int abortExecution(Execution execution, Exception e) {
//        logger.error(e.getMessage(), e);
//        return abortExecution(execution, e.getMessage());
//    }

    private int abortExecution(Execution execution, String description) {
        logger.info("Aborting execution: {} - Reason: '{}'", execution.getId(), description);
        return setStatus(execution, new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED, description));
    }

    private int setStatus(Execution execution, Enums.ExecutionStatus status) {
        ExecutionInternal internal = new ExecutionInternal().setStatus(status);

        try {
            executionManager.updateInternal(execution.getStudy().getId(), execution.getId(), internal, token);
        } catch (CatalogException e) {
            logger.error("Unexpected error. Cannot update execution '{}' to status '{}'. {}", execution.getId(), status.getName(),
                    e.getMessage(), e);
            return 0;
        }

        execution.getInternal().setStatus(status);
        notifyStatusChange(execution);

        return 1;
    }

//    private Enums.ExecutionStatus getCurrentStatus(Execution execution) {
//
//        Path resultJson = getExecutionResultPath(execution);
//
//        // Check if analysis result file is there
//        if (resultJson != null && Files.exists(resultJson)) {
//            ExecutionResult execution = readExecutionResult(resultJson);
//            if (execution != null) {
//                Instant lastStatusUpdate = execution.getStatus().getDate().toInstant();
//                if (lastStatusUpdate.until(Instant.now(), ChronoUnit.MINUTES) > EXECUTION_RESULT_FILE_EXPIRATION_MINUTES) {
//                    logger.warn("Ignoring file '" + resultJson + "'. The file is more than "
//                            + EXECUTION_RESULT_FILE_EXPIRATION_MINUTES + " minutes old");
//                } else {
//                    return new Enums.ExecutionStatus(execution.getStatus().getName().name());
//                }
//            } else {
//                if (Files.exists(resultJson)) {
//                    logger.warn("File '" + resultJson + "' seems corrupted.");
//                } else {
//                    logger.warn("Could not find file '" + resultJson + "'.");
//                }
//            }
//        }
//
//        String status = batchExecutor.getStatus(execution.getId());
//        if (!StringUtils.isEmpty(status) && !status.equals(Enums.ExecutionStatus.UNKNOWN)) {
//            return new Enums.ExecutionStatus(status);
//        } else {
//            Path tmpOutdirPath = Paths.get(execution.getOutDir().getUri());
//            // Check if the error file is present
//            Path errorLog = tmpOutdirPath.resolve(getErrorLogFileName(execution));
//
//            if (Files.exists(errorLog)) {
//                // FIXME: This may not be true. There is a delay between execution starts (i.e. error log appears) and
//                //  the analysis result creation
//
//                // There must be some command line error. The execution started running but did not finish well, otherwise we would find
//                the
//                // analysis-result.yml file
//                return new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Command line error");
//            } else {
//                return new Enums.ExecutionStatus(Enums.ExecutionStatus.QUEUED);
//            }
//        }
//
//    }
//
//    private Path getExecutionResultPath(Execution execution) {
//        Path resultJson = null;
//        try (Stream<Path> stream = Files.list(Paths.get(execution.getOutDir().getUri()))) {
//            resultJson = stream
//                    .filter(path -> {
//                        String str = path.toString();
//                        return str.endsWith(ExecutionResultManager.FILE_EXTENSION)
//                                && !str.endsWith(ExecutionResultManager.SWAP_FILE_EXTENSION);
//                    })
//                    .findFirst()
//                    .orElse(null);
//        } catch (IOException e) {
//            logger.warn("Could not find AnalysisResult file", e);
//        }
//        return resultJson;
//    }

    //    private ExecutionResult readExecutionResult(Execution execution) {
//        Path resultJson = getExecutionResultPath(execution);
//        if (resultJson != null) {
//            return readExecutionResult(resultJson);
//        }
//        return null;
//    }
//
//    private ExecutionResult readExecutionResult(Path file) {
//        if (file == null) {
//            return null;
//        }
//        int attempts = 0;
//        int maxAttempts = 3;
//        while (attempts < maxAttempts) {
//            attempts++;
//            try {
//                try (InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()))) {
//                    return JacksonUtils.getDefaultObjectMapper().readValue(is, ExecutionResult.class);
//                }
//            } catch (IOException e) {
//                if (attempts == maxAttempts) {
//                    logger.error("Could not load AnalysisResult file: " + file.toAbsolutePath(), e);
//                } else {
//                    logger.warn("Could not load AnalysisResult file: " + file.toAbsolutePath()
//                            + ". Retry " + attempts + "/" + maxAttempts
//                            + ". " + e.getMessage()
//                    );
//                    try {
//                        Thread.sleep(100);
//                    } catch (InterruptedException interruption) {
//                        // Ignore interruption
//                        Thread.currentThread().interrupt();
//                    }
//                }
//            }
//        }
//
//        return null;
//    }
//
//    private int processFinishedExecution(Execution execution, Enums.ExecutionStatus status) {
//        logger.info("[{}] - Processing finished execution with status {}", execution.getId(), status.getName());
//
//        Path outDirUri = Paths.get(execution.getOutDir().getUri());
//        Path analysisResultPath = getExecutionResultPath(execution);
//
//        logger.info("[{}] - Registering execution results from '{}'", execution.getId(), outDirUri);
//
//        ExecutionResult execution;
//        if (analysisResultPath != null) {
//            execution = readExecutionResult(analysisResultPath);
//            if (execution != null) {
//                PrivateExecutionUpdateParams updateParams = new PrivateExecutionUpdateParams().setExecution(execution);
//                try {
//                    executionManager.update(execution.getStudy().getId(), execution.getId(), updateParams, QueryOptions.empty(), token);
//                } catch (CatalogException e) {
//                    logger.error("[{}] - Catastrophic error. Could not update execution information with final result {}: {}",
//                    execution.getId(),
//                            updateParams.toString(), e.getMessage(), e);
//                    return 0;
//                }
//            }
//        } else {
//            execution = null;
//        }
//
//        List<File> registeredFiles;
//        try {
//            Predicate<URI> uriPredicate = uri -> !uri.getPath().endsWith(ExecutionResultManager.FILE_EXTENSION)
//                    && !uri.getPath().endsWith(ExecutionResultManager.SWAP_FILE_EXTENSION)
//                    && !uri.getPath().contains("/scratch_");
//            registeredFiles = fileManager.syncUntrackedFiles(execution.getStudy().getId(), execution.getOutDir().getPath(), uriPredicate,
//            execution.getId(),
//                    token).getResults();
//        } catch (CatalogException e) {
//            logger.error("Could not registered files in Catalog: {}", e.getMessage(), e);
//            return 0;
//        }
//
//        // Register the execution information
//        PrivateExecutionUpdateParams updateParams = new PrivateExecutionUpdateParams();
//
//        // Process output and log files
//        List<File> outputFiles = new LinkedList<>();
//        String logFileName = getLogFileName(execution);
//        String errorLogFileName = getErrorLogFileName(execution);
//        for (File registeredFile : registeredFiles) {
//            if (registeredFile.getName().equals(logFileName)) {
//                logger.info("[{}] - stdout file '{}'", execution.getId(), registeredFile.getUri().getPath());
//                updateParams.setStdout(registeredFile);
//            } else if (registeredFile.getName().equals(errorLogFileName)) {
//                logger.info("[{}] - stderr file: '{}'", execution.getId(), registeredFile.getUri().getPath());
//                updateParams.setStderr(registeredFile);
//            } else {
//                outputFiles.add(registeredFile);
//            }
//        }
//        if (execution != null) {
//            for (URI externalFile : execution.getExternalFiles()) {
//                Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), externalFile);
//                try {
//                    OpenCGAResult<File> search = fileManager.search(execution.getStudy().getId(), query,
//                    FileManager.INCLUDE_FILE_URI_PATH,
//                            token);
//                    if (search.getNumResults() == 0) {
//                        throw new CatalogException("File not found");
//                    }
//                    outputFiles.add(search.first());
//                } catch (CatalogException e) {
//                    logger.error("Could not obtain external file {}: {}", externalFile, e.getMessage(), e);
//                    return 0;
//                }
//            }
//        }
//        updateParams.setOutput(outputFiles);
//
//
//        updateParams.setInternal(new ExecutionInternal());
//        // Check status of analysis result or if there are files that could not be moved to outdir to decide the final result
//        if (execution == null) {
//            updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR,
//                    "Execution could not finish successfully. Missing execution result"));
//        } else if (execution.getStatus().getName().equals(Status.Type.ERROR)) {
//            updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR,
//                    "Execution could not finish successfully"));
//        } else {
//            switch (status.getName()) {
//                case Enums.ExecutionStatus.DONE:
//                case Enums.ExecutionStatus.READY:
//                    updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.DONE));
//                    break;
//                case Enums.ExecutionStatus.ABORTED:
//                    updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, "Execution aborted!"));
//                    break;
//                case Enums.ExecutionStatus.ERROR:
//                default:
//                    updateParams.getInternal().setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR,
//                            "Execution could not finish successfully"));
//                    break;
//            }
//        }
//
//        logger.info("[{}] - Updating execution information", execution.getId());
//        // We update the execution information
//        try {
//            executionManager.update(execution.getStudy().getId(), execution.getId(), updateParams, QueryOptions.empty(), token);
//        } catch (CatalogException e) {
//            logger.error("[{}] - Catastrophic error. Could not update execution information with final result {}: {}", execution.getId(),
//                    updateParams.toString(), e.getMessage(), e);
//            return 0;
//        }
//
//        execution.getInternal().setStatus(updateParams.getInternal().getStatus());
//        notifyStatusChange(execution);
//
//        // If it is a template, we will store the execution results in the same template folder
//        String toolId = execution.getTool().getId();
//        if (toolId.equals(TemplateRunner.ID)) {
//            copyExecutionResultsInTemplateFolder(execution, outDirUri);
//        }
//
//        return 1;
//    }
//
//    private void copyExecutionResultsInTemplateFolder(Execution execution, Path outDirPath) {
//        try {
//            String templateId = String.valueOf(execution.getParams().get("id"));
//
//            // We obtain the basic studyPath where we will upload the file temporarily
//            Study study = catalogManager.getStudyManager().get(execution.getStudy().getId(),
//                    new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key()), token).first();
//
//            java.nio.file.Path studyPath = Paths.get(study.getUri());
//            Path path = studyPath.resolve("OPENCGA").resolve("TEMPLATE").resolve(templateId);
//
//            FileUtils.copyDirectory(outDirPath.toFile(), path.resolve(outDirPath.getFileName()).toFile());
//            FileUtils.copyFile(outDirPath.resolve(TemplateRunner.ID + ".result.json").toFile(),
//                    path.resolve(TemplateRunner.ID + ".result.json").toFile());
//        } catch (CatalogException | IOException e) {
//            logger.error("Could not store execution results in template folder", e);
//        }
//    }
//
    private void notifyStatusChange(Execution execution) {
        if (execution.getInternal().getWebhook().getUrl() != null) {
            executor.submit(() -> {
                try {
                    sendWebhookNotification(execution, execution.getInternal().getWebhook().getUrl());
                } catch (URISyntaxException | CatalogException | CloneNotSupportedException e) {
                    logger.warn("Could not store notification status: {}", e.getMessage(), e);
                }
            });
        }
    }

    private void sendWebhookNotification(Execution execution, URL url)
            throws URISyntaxException, CatalogException, CloneNotSupportedException {
//        ExecutionInternal executionInternal = new ExecutionInternal(null, null, null, execution.getInternal().getWebhook().clone(), null);
//        PrivateExecutionUpdateParams updateParams = new PrivateExecutionUpdateParams()
//                .setInternal(executionInternal);
//
//        Map<String, Object> actionMap = new HashMap<>();
//        actionMap.put(ExecutionDBAdaptor.QueryParams.INTERNAL_EVENTS.key(), ParamUtils.BasicUpdateAction.ADD.name());
//        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);
//
//        Client client = ClientBuilder.newClient();
//        Response post;
//        try {
//            post = client
//                    .target(url.toURI())
//                    .request(MediaType.APPLICATION_JSON)
//                    .property(ClientProperties.CONNECT_TIMEOUT, 1000)
//                    .property(ClientProperties.READ_TIMEOUT, 5000)
//                    .post(Entity.json(execution));
//        } catch (ProcessingException e) {
//            executionInternal.getWebhook().getStatus().put(execution.getInternal().getStatus().getName(),
//            ExecutionInternalWebhook.Status.ERROR);
//            executionInternal.setEvents(Collections.singletonList(new Event(Event.Type.ERROR, "Could not notify through webhook. "
//                    + e.getMessage())));
//
//            executionManager.update(execution.getStudy().getId(), execution.getId(), updateParams, options, token);
//
//            return;
//        }
//        if (post.getStatus() == HttpStatus.SC_OK) {
//            executionInternal.getWebhook().getStatus().put(execution.getInternal().getStatus().getName(),
//            ExecutionInternalWebhook.Status.SUCCESS);
//        } else {
//            executionInternal.getWebhook().getStatus().put(execution.getInternal().getStatus().getName(),
//            ExecutionInternalWebhook.Status.ERROR);
//            executionInternal.setEvents(Collections.singletonList(new Event(Event.Type.ERROR, "Could not notify through webhook.
//            HTTP response "
//                    + "code: " + post.getStatus())));
//        }
//
//        executionManager.update(execution.getStudy().getId(), execution.getId(), updateParams, options, token);
    }

    private String getErrorLogFileName(Execution execution) {
        return execution.getId() + ".err";
    }

    private String getLogFileName(Execution execution) {
        // WARNING: If we change the way we name log files, we will also need to change it in the "log" method from the ExecutionManager !!
        return execution.getId() + ".log";
    }

}
