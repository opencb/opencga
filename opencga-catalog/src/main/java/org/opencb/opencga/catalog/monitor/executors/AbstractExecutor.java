package org.opencb.opencga.catalog.monitor.executors;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.opencb.opencga.catalog.models.Job;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by pfurio on 22/08/16.
 */
public abstract class AbstractExecutor {

    public static final String TIMEOUT = "timeout";
    public static final String STDOUT = "stdout";
    public static final String STDERR = "stderr";
    public static final String OUTDIR = "outdir";
    public static final String NUM_THREADS = "num_threads";
    public static final String MAX_MEM = "max_mem";
    public static final String JOB_STATUS_FILE = "job.status";

    protected Logger logger;
    protected ObjectMapper objectMapper;
    protected ObjectReader objectReader;

    public AbstractExecutor() {
        objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader(Job.JobStatus.class);
    }

    public abstract void execute(Job job) throws Exception;

    public String status(Path jobOutput, Job job) {
        Path jobStatusFilePath = jobOutput.resolve(JOB_STATUS_FILE);
        if (!jobStatusFilePath.toFile().exists()) {
            return getStatus(job);
        }
        // File exists
        try {
            Job.JobStatus jobStatus = objectReader.readValue(jobStatusFilePath.toFile());
            return jobStatus.getName();
        } catch (IOException e) {
            logger.warn("Job status file could not be read.");
            return getStatus(job);
        }
    }

    protected abstract String getStatus(Job job);

    public abstract boolean stop(Job job) throws Exception;

    public abstract boolean resume(Job job) throws Exception;

    public abstract boolean kill(Job job) throws Exception;

    public abstract boolean isExecutorAlive();

    protected ExecutorConfig getExecutorConfig(Job job) {
        ExecutorConfig executorConfig = null;

        if (job != null && job.getResourceManagerAttributes() != null) {
            executorConfig = new ExecutorConfig();

            if (job.getResourceManagerAttributes().get(STDOUT) != null) {
                executorConfig.setStdout(job.getResourceManagerAttributes().get(STDOUT).toString());
            }

            if (job.getResourceManagerAttributes().get(STDERR) != null) {
                executorConfig.setStderr(job.getResourceManagerAttributes().get(STDERR).toString());
            }

            if (job.getResourceManagerAttributes().get(OUTDIR) != null) {
                executorConfig.setOutdir(job.getResourceManagerAttributes().get(OUTDIR).toString());
            }

            if (job.getResourceManagerAttributes().get(TIMEOUT) != null) {
                executorConfig.setTimeout(Integer.parseInt(job.getResourceManagerAttributes().get(TIMEOUT).toString()));
            }

            if (job.getResourceManagerAttributes().get(MAX_MEM) != null) {
                executorConfig.setMaxMem(Integer.parseInt(job.getResourceManagerAttributes().get(MAX_MEM).toString()));
            }

            if (job.getResourceManagerAttributes().get(NUM_THREADS) != null) {
                executorConfig.setNumThreads(Integer.parseInt(job.getResourceManagerAttributes().get(NUM_THREADS).toString()));
            }
        }

        return executorConfig;
    }

}
