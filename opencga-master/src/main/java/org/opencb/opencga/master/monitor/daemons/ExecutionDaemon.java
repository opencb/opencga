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
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.analysis.tools.ToolFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ExecutionManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.*;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.net.URL;
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
    private final ExecutionManager executionManager;

    // Maximum number of executions of each type (Pending, queued, running) that will be handled on each iteration.
    // Example: If there are 100 pending executions, 15 queued, 70 running.
    // On first iteration, it will queue 50 out of the 100 pending executions. It will check up to 50 queue-running changes out of the 65
    // (15 + 50 from pending), and it will check up to 50 finished executions from the running ones.
    // On second iteration, it will queue the remaining 50 pending executions, and so on...
    private static final int NUM_HANDLED = 50;
    private final Query pendingExecutionsQuery;
    private final Query processedExecutionsQuery;
    private final Query queuedExecutionsQuery;
    private final Query runningExecutionsQuery;

    private final QueryOptions queryOptions;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public ExecutionDaemon(int interval, String token, CatalogManager catalogManager) throws CatalogDBException {
        super(interval, token, catalogManager);

        this.executionManager = catalogManager.getExecutionManager();

        pendingExecutionsQuery = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PENDING);
        processedExecutionsQuery = new Query(ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Enums.ExecutionStatus.PROCESSED);
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
        long processedExecutions = -1;
        long queuedExecutions = -1;
        long runningExecutions = -1;
        try {
            pendingExecutions = executionManager.count(pendingExecutionsQuery, token).getNumMatches();
            processedExecutions = executionManager.count(processedExecutionsQuery, token).getNumMatches();
            queuedExecutions = executionManager.count(queuedExecutionsQuery, token).getNumMatches();
            runningExecutions = executionManager.count(runningExecutionsQuery, token).getNumMatches();
        } catch (CatalogException e) {
            logger.error("{}", e.getMessage(), e);
        }
        logger.info("----- JOB DAEMON  ----- pending={}, processed={}, queued={}, running={}", pendingExecutions, processedExecutions,
                queuedExecutions, runningExecutions);

            /*
            PENDING JOBS
             */
        if (pendingExecutions > 0) {
            checkPendingExecutions();
        }

            /*
            PROCESSED JOBS
             */
        if (processedExecutions > 0) {
            checkExecutionsStatuses(processedExecutionsQuery);
        }

            /*
            QUEUED JOBS
             */
        if (queuedExecutions > 0) {
            checkExecutionsStatuses(queuedExecutionsQuery);
        }

            /*
            RUNNING JOBS
             */
        if (runningExecutions > 0) {
            checkExecutionsStatuses(runningExecutionsQuery);
        }
    }

    protected void checkExecutionsStatuses(Query query) {
        int handledProcessedExecutions = 0;
        try (DBIterator<Execution> iterator = executionManager.iterator(query, queryOptions, token)) {
            while (handledProcessedExecutions < NUM_HANDLED && iterator.hasNext()) {
                try {
                    Execution execution = iterator.next();
                    handledProcessedExecutions += checkExecutionStatus(execution);
                } catch (Exception e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage(), e);
        }
    }

    /**
     * Check if processed executions can be moved to queued or running.
     *
     * @param execution Execution object.
     * @return 1 if the execution has changed the status, 0 otherwise.
     */
    protected int checkExecutionStatus(Execution execution) {
        if (StringUtils.isEmpty(execution.getStudy().getId())) {
            return abortExecution(execution, "Missing mandatory 'studyUuid' field");
        }

        String status = getCurrentExecutionStatus(execution);
        if (!status.equals(execution.getInternal().getStatus().getName())) {
            setStatus(execution, new Enums.ExecutionStatus(status));
            return 1;
        } else {
            return 0;
        }
    }

//    private String getCurrentExecutionStatus(Execution execution) {
//        Map<String, Integer> statusMap = new HashMap<>();
//        statusMap.put(Enums.ExecutionStatus.PENDING, 0);
//        statusMap.put(Enums.ExecutionStatus.RUNNING, 0);
//        statusMap.put(Enums.ExecutionStatus.ERROR, 0);
//        statusMap.put(Enums.ExecutionStatus.DONE, 0);
//        statusMap.put(Enums.ExecutionStatus.QUEUED, 0);
//        statusMap.put(Enums.ExecutionStatus.ABORTED, 0);
//
//        for (Job job : execution.getJobs()) {
//            String status = job.getInternal().getStatus().getName();
//            switch(status) {
//                case Enums.ExecutionStatus.PENDING:
//                case Enums.ExecutionStatus.RUNNING:
//                case Enums.ExecutionStatus.ERROR:
//                case Enums.ExecutionStatus.DONE:
//                case Enums.ExecutionStatus.QUEUED:
//                case Enums.ExecutionStatus.ABORTED:
//                    statusMap.put(status, statusMap.get(status) + 1);
//                    break;
//                default:
//                    throw new RuntimeException("Unexpected job status in job '" + job.getId() + "': " + status);
//            }
//        }
//
//        int pendingJobs = statusMap.get(Enums.ExecutionStatus.PENDING) + statusMap.get(Enums.ExecutionStatus.QUEUED)
//                + statusMap.get(Enums.ExecutionStatus.RUNNING);
//
//        // Check if there are any pending jobs
//        if (pendingJobs > 0) {
//            if ()
//        }
//
//
//    }


    private String getCurrentExecutionStatus(Execution execution) {
        int pendingJobs = 0;
        int runningJobs = 0;
        int errorJobs = 0;
//        int doneJobs = 0;
        int queuedJobs = 0;
        int abortedJobs = 0;

        for (Job job : execution.getJobs()) {
            String status = job.getInternal().getStatus().getName();
            switch (status) {
                case Enums.ExecutionStatus.PENDING:
                    pendingJobs++;
                    break;
                case Enums.ExecutionStatus.RUNNING:
                    runningJobs++;
                    break;
                case Enums.ExecutionStatus.ERROR:
                    errorJobs++;
                    break;
                case Enums.ExecutionStatus.DONE:
//                    doneJobs++;
                    break;
                case Enums.ExecutionStatus.QUEUED:
                    queuedJobs++;
                    break;
                case Enums.ExecutionStatus.ABORTED:
                    abortedJobs++;
                    break;
                default:
                    throw new RuntimeException("Unexpected job status in job '" + job.getId() + "': " + status);
            }
        }

        if (errorJobs > 0 || abortedJobs > 0) {
            return Enums.ExecutionStatus.ERROR;
        }
        if (runningJobs > 0) {
            return Enums.ExecutionStatus.RUNNING;
        }
        if (queuedJobs > 0) {
            return Enums.ExecutionStatus.QUEUED;
        }
        if (pendingJobs > 0) {
            return Enums.ExecutionStatus.PROCESSED;
        }
        return Enums.ExecutionStatus.DONE;
    }

    protected void checkPendingExecutions() {
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

        if (execution.getPipeline() == null) {
            return abortExecution(execution, "Execution doesn't look like a pipeline.");
        }

        if (!canBeQueued(execution)) {
            return 0;
        }

        // Check job status
        Set<String> allJobs = new HashSet<>();
        Set<String> finishedJobs = new HashSet<>();
        for (Job job : execution.getJobs()) {
            String pipelineJobId = job.getTool().getId();
            switch (job.getInternal().getStatus().getName()) {
                case Enums.ExecutionStatus.ERROR:
                case Enums.ExecutionStatus.ABORTED:
                    return failExecution(execution, "Job '" + pipelineJobId + "' failed with description: "
                            + job.getInternal().getStatus().getDescription());
                case Enums.ExecutionStatus.DONE:
                    finishedJobs.add(pipelineJobId);
                    allJobs.add(pipelineJobId);
                    break;
                default:
                    allJobs.add(pipelineJobId);
                    break;
            }
        }

        List<Job> jobList = new LinkedList<>();
        int skippedJobs = 0;
        for (Map.Entry<String, Pipeline.PipelineJob> jobEntry : execution.getPipeline().getJobs().entrySet()) {
            String toolId = getPipelineJobId(jobEntry.getKey(), jobEntry.getValue());
            if (allJobs.contains(toolId)) {
                // Skip job. This job was already processed and it is already registered
                continue;
            }

            Pipeline.PipelineJob pipelineJob = jobEntry.getValue();
            List<String> dependsOn = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(pipelineJob.getDependsOn())) {
                boolean createJob = true;
                for (String jobDependsOn : pipelineJob.getDependsOn()) {
                    if (!finishedJobs.contains(jobDependsOn)) {
                        createJob = false;
                    }
                }
                if (!createJob) {
                    skippedJobs++;
                    continue;
                }
            }

            Map<String, Object> jobParams;
            try {
                jobParams = getJobParams(execution.getParams(), pipelineJob.getParams(), execution.getPipeline().getParams(), toolId);
            } catch (ToolException e) {
                return abortExecution(execution, e.getMessage());
            }
            Job job = createJobInstance(execution.getId(), toolId, pipelineJob.getDescription(), execution.getPriority(), jobParams,
                    execution.getTags(), dependsOn, execution.getUserId());
            jobList.add(job);
        }

        if (!jobList.isEmpty()) {
            try {
                catalogManager.getJobManager().submit(execution.getStudy().getId(), jobList, token);
            } catch (CatalogException e) {
                return abortExecution(execution, e.getMessage());
            }

            if (skippedJobs == 0) {
                // Update execution (new status and new list of jobs?)
                setStatus(execution,
                        new Enums.ExecutionStatus(Enums.ExecutionStatus.PROCESSED, "Execution has been processed and all jobs created"));
            } else {
                setStatus(execution,
                        new Enums.ExecutionStatus(Enums.ExecutionStatus.PENDING, "Execution has been partially processed and a few jobs "
                                + "launched. Number of unprocessed jobs: " + skippedJobs));
            }
            return 1;
        }

        return 0;
    }

    private String getPipelineJobId(String key, Pipeline.PipelineJob pipelineJob) {
        if (StringUtils.isNotEmpty(pipelineJob.getToolId())) {
            return pipelineJob.getToolId();
        }
        if (pipelineJob.getExecutable() != null && StringUtils.isNotEmpty(pipelineJob.getExecutable().getId())) {
            return pipelineJob.getExecutable().getId();
        }
        return key;
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
        // User params
        Map<String, Object> params = userParams != null ? new HashMap<>(userParams) : new HashMap<>();
        if (jobParams != null) {
            // Merge with job params
            jobParams.forEach(params::putIfAbsent);
        }
        if (pipelineParams != null) {
            // Merge with pipeline params
            pipelineParams.forEach(params::putIfAbsent);
        }
        replaceCodedParamsValues(params);

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
        Map<String, Object> finalParams = new HashMap<>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (jobAllowedParams.contains(entry.getKey())) {
                finalParams.put(entry.getKey(), entry.getValue());
            }
        }

        return finalParams;
    }

    /**
     * Method to fully replace all the fields with their expected values.
     * Example: bamFile : /genomes/alignment.bam
     * file:     $PIPELINE.params.bamFile
     * <p>
     * Result:  bamFile : /genomes/alignment.bam
     * file:     /genomes/alignment.bam
     *
     * @param params Parameters map.
     */
    private void replaceCodedParamsValues(Map<String, Object> params) {
        // Map containing key - keyToBeReplacedWith where key is the parameter field accepted by the job and keyToBeReplacedWith the field
        // containing the value that it needs to contain 'key'
        Map<String, String> replacedKeyWithMap = new HashMap<>();
        for (String paramKey : params.keySet()) {
            String value = String.valueOf(params.get(paramKey));
            if (value.startsWith("$PIPELINE.params.")) {
                String key = value.replace("$PIPELINE.params.", "");
                replacedKeyWithMap.put(paramKey, key);
            }
        }

        for (Map.Entry<String, String> entry : replacedKeyWithMap.entrySet()) {
            if (params.containsKey(entry.getValue())) {
                params.put(entry.getKey(), params.get(entry.getValue()));
            }
        }
    }

    private boolean canBeQueued(Execution execution) {
        if (execution.getDependsOn() != null && !execution.getDependsOn().isEmpty()) {
            for (Execution tmpExecution : execution.getDependsOn()) {
                if (!Enums.ExecutionStatus.DONE.equals(tmpExecution.getInternal().getStatus().getName())) {
                    if (Enums.ExecutionStatus.ABORTED.equals(tmpExecution.getInternal().getStatus().getName())
                            || Enums.ExecutionStatus.ERROR.equals(tmpExecution.getInternal().getStatus().getName())) {
                        abortExecution(execution, "The execution '" + tmpExecution.getId() + "' it depended on finished with status '"
                                + tmpExecution.getInternal().getStatus().getName() + "'");
                    }
                    return false;
                }
            }
        }

        return true;

//        if (!batchExecutor.canBeQueued()) {
//            return false;
//        }

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

    private int failExecution(Execution execution, String description) {
        logger.info("Stopping execution with ERROR: {} - Reason: '{}'", execution.getId(), description);
        return setStatus(execution, new Enums.ExecutionStatus(Enums.ExecutionStatus.ERROR, description));
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

}
