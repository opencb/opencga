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

package org.opencb.opencga.master.monitor.executors;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import org.opencb.opencga.core.config.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Created by wasim on 17/12/18.
 */
public class AzureBatchExecutor implements BatchExecutor {

    private String batchAccount;
    private String batchKey;
    private String batchUri;
    private String batchPoolId;
    private String dockerImageName;
    private String dockerArgs;
    private BatchClient batchClient;
    private PoolInformation poolInformation;

    private static Logger logger;

    private static final String VARIANT_INDEX_JOB = "variant-index-job";
    private static final String VARIANT_ANALYSIS_JOB = "variant-analysis-job";
    private static final String BATCH_ACCOUNT = "azure.batchAccount";
    private static final String BATCH_KEY = "azure.batchKey";
    private static final String BATCH_URI = "azure.batchUri";
    private static final String BATCH_POOL_ID = "azure.batchPoolId";
    private static final String DOCKER_IMAGE_NAME = "azure.dockerImageName";
    private static final String DOCKER_ARGS = "azure.dockerArgs";


    public AzureBatchExecutor(Execution execution) {
        logger = LoggerFactory.getLogger(AzureBatchExecutor.class);
        populateOptions(execution);
        this.batchClient = createBatchClient();
        this.poolInformation = new PoolInformation().withPoolId(batchPoolId);
    }

//    public void submitAzureTask(Job job, String token) throws IOException {
//        String jobId = getOrCreateAzureJob(job.getType());
//        TaskAddParameter taskToAdd = new TaskAddParameter();
//        taskToAdd.withId(job.getId()).withCommandLine(job.getCommandLine()).withContainerSettings(
//                new TaskContainerSettings()
//                        .withImageName(dockerImageName)
//                        .withContainerRunOptions(dockerArgs));
//        taskToAdd.withId(job.getId()).withCommandLine(getCommandLine(job, token));
//        batchClient.taskOperations().createTask(jobId, taskToAdd);
//    }
//
//    private String getOrCreateAzureJob(Job.Type jobType) throws IOException {
//        String job = getAzureJobType(jobType);
//        PagedList<CloudJob> cloudJobs = batchClient.jobOperations().listJobs();
//        for (CloudJob cloudJob : cloudJobs) {
//            if (cloudJob.id().equals(job)) {
//                return job;
//            }
//        }
//        batchClient.jobOperations().createJob(job, this.poolInformation);
//        return job;
//    }
//
//    private String getAzureJobType(Job.Type jobType) {
//        return jobType == Job.Type.ANALYSIS ? VARIANT_ANALYSIS_JOB : VARIANT_INDEX_JOB;
//    }

    private BatchClient createBatchClient() {
        return BatchClient.open(new BatchSharedKeyCredentials(batchUri, batchAccount, batchKey));
    }

    @Override
    public void execute(String studyId, String jobId, String queue, String commandLine, Path stdout, Path stderr) throws Exception {
//        submitAzureTask(job, token);
    }

    @Override
    public String getStatus(String studyId, String jobId) {
        return null;
//        try {
//            CloudTask cloudTask = batchClient.taskOperations().getTask(getAzureJobType(job.getType()), job.getId());
//            TaskState state = cloudTask.state();
//            switch (state) {
//                case RUNNING:
//                    return Job.JobStatus.RUNNING;
//                case COMPLETED:
//                    return Job.JobStatus.DONE;
//                case ACTIVE:
//                case PREPARING:
//                    return Job.JobStatus.QUEUED;
//                default:
//                    return Job.JobStatus.UNKNOWN;
//            }
//        } catch (BatchErrorException | IOException e) {
//            logger.error("unable to get azure task status {}", job.getId());
//            return Job.JobStatus.UNKNOWN;
//        }
    }

    @Override
    public boolean stop(String studyId, String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean resume(String studyId, String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean kill(String studyId, String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean isExecutorAlive() {
        return false;
    }

    // configuration values from configuration.yml file
    private void populateOptions(Execution execution) {
        batchAccount = execution.getOptions().getString(BATCH_ACCOUNT);
        batchKey = execution.getOptions().getString(BATCH_KEY);
        batchUri = execution.getOptions().getString(BATCH_URI);
        batchPoolId = execution.getOptions().getString(BATCH_POOL_ID);
        dockerImageName = execution.getOptions().getString(DOCKER_IMAGE_NAME);
        dockerArgs = execution.getOptions().getString(DOCKER_ARGS);
    }
}
