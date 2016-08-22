package org.opencb.opencga.catalog.monitor.executors;

import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.monitor.exceptions.ExecutionException;
import org.slf4j.Logger;

/**
 * Created by pfurio on 22/08/16.
 */
public abstract class ParentExecutor {

    public static final String TIMEOUT = "TIMEOUT";
    public static final String STDOUT = "STDOUT";
    public static final String STDERR = "STDERR";
    public static final String NUM_THREADS = "NUM_THREADS";
    public static final String MAX_MEM = "MAX_MEM";

    protected Logger logger;

    public abstract void execute(Job job) throws Exception;

    public abstract String status(Job job) throws ExecutionException;

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
