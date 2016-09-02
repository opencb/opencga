package org.opencb.opencga.catalog.monitor.executors;

import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.SgeManager;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by pfurio on 24/08/16.
 */
public class SGEExecutor extends AbstractExecutor {

    private SGEManager sgeManager;

    public SGEExecutor(CatalogConfiguration catalogConfiguration) {
        logger = LoggerFactory.getLogger(SGEExecutor.class);
        sgeManager = new SGEManager(catalogConfiguration.getExecution());
    }

    @Override
    public void execute(Job job) throws Exception {
        ExecutorConfig executorConfig = getExecutorConfig(job);
        // TODO: Check job name below !
        sgeManager.queueJob(job.getToolName(), "", -1, job.getCommandLine(), executorConfig);
    }

    @Override
    protected String getStatus(Job job) {
        String status;
        try {
            status = SgeManager.status(Objects.toString(job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME)));
        } catch (Exception e) {
            logger.error("Could not obtain the status of the job {} from SGE. Error: {}", job.getId(), e.getMessage());
            return Job.JobStatus.UNKNOWN;
        }

        switch (status) {
            case SgeManager.ERROR:
            case SgeManager.EXECUTION_ERROR:
                return Job.JobStatus.ERROR;
            case SgeManager.FINISHED:
                return Job.JobStatus.READY;
            case SgeManager.QUEUED:
                return Job.JobStatus.QUEUED;
            case SgeManager.RUNNING:
            case SgeManager.TRANSFERRED:
                return Job.JobStatus.RUNNING;
            case SgeManager.UNKNOWN:
            default:
                return job.getStatus().getName();
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
}
