package org.opencb.opencga.catalog.monitor.executors;

import com.microsoft.azure.PagedList;
import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.*;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by wasim on 17/12/18.
 */
public class AzureBatchExecutor implements BatchExecutor {
    public static final String VARIANT_INDEX_JOB = "variant-index-job";
    public static final String VARIANT_ANALYSIS_JOB = "variant-analysis-job";
    public static final String BATCH_ACCOUNT = "batchAccount";
    public static final String BATCH_KEY = "batchKey";
    public static final String BATCH_URI = "batchUri";
    public static final String BATCH_POOL_ID = "batchPoolId";
    public static final String DOCKER_IMAGE_NAME = "dockerImageName";
    public static final String DOCKER_ARGS = "dockerArgs";
    private static Logger logger;

    private String batchAccount;
    private String batchKey;
    private String batchUri;
    private String batchPoolId;
    private String dockerImageName;
    private String dockerArgs;
    private BatchClient batchClient;
    private PoolInformation poolInformation;

    public AzureBatchExecutor(Configuration configuration) {
        logger = LoggerFactory.getLogger(AzureBatchExecutor.class);
        populateOptions(configuration);
        this.batchClient = createBatchClient();
        this.poolInformation = new PoolInformation().withPoolId(batchPoolId);
    }

    public void submitAzureTask(Job job) throws IOException {
        String jobId = getOrCreateAzureJob(job.getType());
        TaskAddParameter taskToAdd = new TaskAddParameter();
        taskToAdd.withId(job.getId()).withCommandLine(job.getCommandLine()).withContainerSettings(
                new TaskContainerSettings().withImageName(dockerImageName)
                        .withContainerRunOptions(dockerArgs));
        taskToAdd.withId(job.getId()).withCommandLine(job.getCommandLine());
        batchClient.taskOperations().createTask(jobId, taskToAdd);
    }

    private String getOrCreateAzureJob(Job.Type jobType) throws IOException {
        String job = getAzureJobType(jobType);
        PagedList<CloudJob> cloudJobs = batchClient.jobOperations().listJobs();
        for (CloudJob cloudJob : cloudJobs) {
            if (cloudJob.id().equals(job)) {
                return job;
            }
        }
        batchClient.jobOperations().createJob(job, this.poolInformation);
        return job;
    }

    private String getAzureJobType(Job.Type jobType) {
        return jobType == Job.Type.ANALYSIS ? VARIANT_ANALYSIS_JOB : VARIANT_INDEX_JOB;
    }

    private BatchClient createBatchClient() {
        return BatchClient.open(new BatchSharedKeyCredentials(batchUri, batchAccount, batchKey));
    }

    @Override
    public void execute(Job job, String token) throws Exception {
        submitAzureTask(job);
    }

    @Override
    public String getStatus(Job job) {
        CloudTask cloudTask;
        try {
            cloudTask = batchClient.taskOperations().getTask(getAzureJobType(job.getType()), job.getId());
        } catch (BatchErrorException | IOException e) {
            logger.error("unable to get azure task status {}", job.getId());
            return Job.JobStatus.UNKNOWN;
        }

        TaskState state = cloudTask.state();
        switch (state) {
            case RUNNING:
                return Job.JobStatus.RUNNING;
            case COMPLETED:
                return Job.JobStatus.DONE;
            case ACTIVE:
            case PREPARING:
                return Job.JobStatus.QUEUED;
            default:
                return Job.JobStatus.UNKNOWN;
        }
    }

    @Override
    public boolean stop(Job job) throws Exception {
        return false;
    }

    @Override
    public boolean resume(Job job) throws Exception {
        return false;
    }

    @Override
    public boolean kill(Job job) throws Exception {
        return false;
    }

    @Override
    public boolean isExecutorAlive() {
        return false;
    }

    private void populateOptions(Configuration configuration) {
        batchAccount = configuration.getExecution().getOptions().get(BATCH_ACCOUNT);
        batchKey = configuration.getExecution().getOptions().get(BATCH_KEY);
        batchUri = configuration.getExecution().getOptions().get(BATCH_URI);
        batchPoolId = configuration.getExecution().getOptions().get(BATCH_POOL_ID);
        dockerImageName = configuration.getExecution().getOptions().get(DOCKER_IMAGE_NAME);
        dockerArgs = configuration.getExecution().getOptions().get(DOCKER_ARGS);
    }
}
