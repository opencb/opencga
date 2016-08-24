package org.opencb.opencga.catalog.monitor.executors;

import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.RunnableProcess;
import org.opencb.opencga.catalog.models.Job;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by pfurio on 22/08/16.
 */
public class LocalExecutor extends AbstractExecutor {

    public LocalExecutor() {
        logger = LoggerFactory.getLogger(LocalExecutor.class);
    }

    @Override
    public void execute(Job job) throws Exception {
        Runnable runnable = () -> {
            try {
                ExecutorConfig executorConfig = getExecutorConfig(job);

                logger.info("Ready to run {}", job.getCommandLine());
                Command com = new Command(job.getCommandLine());

                DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(executorConfig.getStdout()));
                com.setOutputOutputStream(dataOutputStream);

                dataOutputStream = new DataOutputStream(new FileOutputStream(executorConfig.getStderr()));
                com.setErrorOutputStream(dataOutputStream);

                final long jobId = job.getId();

                Thread hook = new Thread(() -> {
                    logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
                    com.setStatus(RunnableProcess.Status.KILLED);
                    com.setExitValue(-2);
                    closeOutputStreams(com);
                });

                logger.info("==========================================");
                logger.info("Executing job {}({})", job.getName(), job.getId());
                logger.debug("Executing commandLine {}", job.getCommandLine());
                logger.info("==========================================");
                System.err.println();

                Runtime.getRuntime().addShutdownHook(hook);
                com.run();
                Runtime.getRuntime().removeShutdownHook(hook);

                System.err.println();
                logger.info("==========================================");
                logger.info("Finished job {}({})", job.getName(), job.getId());
                logger.info("==========================================");

                closeOutputStreams(com);
            } catch (FileNotFoundException e) {
                logger.error("Could not create the output/error files");
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

    @Override
    protected String getStatus(Job job) {
        return Job.JobStatus.UNKNOWN;
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


    private void closeOutputStreams(Command com) {
        /** Close output streams **/
        if (com.getOutputOutputStream() != null) {
            try {
                com.getOutputOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            com.setOutputOutputStream(null);
            com.setOutput(null);
        }
        if (com.getErrorOutputStream() != null) {
            try {
                com.getErrorOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            com.setErrorOutputStream(null);
            com.setError(null);
        }
    }
}
