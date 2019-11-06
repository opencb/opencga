package org.opencb.opencga.catalog.monitor.executors;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Job;
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
    private static final String BATCH_ACCOUNT = "batchAccount";
    private static final String BATCH_KEY = "batchKey";
    private static final String BATCH_URI = "batchUri";
    private static final String BATCH_POOL_ID = "batchPoolId";
    private static final String DOCKER_IMAGE_NAME = "dockerImageName";
    private static final String DOCKER_ARGS = "dockerArgs";


    public AzureBatchExecutor(Configuration configuration) {
        logger = LoggerFactory.getLogger(AzureBatchExecutor.class);
        populateOptions(configuration);
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
    public void execute(Job job, String token) throws Exception {
//        submitAzureTask(job, token);
    }

    @Override
    public void execute(String jobId, String commandLine, Path stdout, Path stderr, String token) throws Exception {
    }

    @Override
    public String getStatus(Job job) {
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

    // configuration values from configuration.yml file
    private void populateOptions(Configuration configuration) {
        batchAccount = configuration.getExecution().getOptions().get(BATCH_ACCOUNT);
        batchKey = configuration.getExecution().getOptions().get(BATCH_KEY);
        batchUri = configuration.getExecution().getOptions().get(BATCH_URI);
        batchPoolId = configuration.getExecution().getOptions().get(BATCH_POOL_ID);
        dockerImageName = configuration.getExecution().getOptions().get(DOCKER_IMAGE_NAME);
        dockerArgs = configuration.getExecution().getOptions().get(DOCKER_ARGS);
    }
}
