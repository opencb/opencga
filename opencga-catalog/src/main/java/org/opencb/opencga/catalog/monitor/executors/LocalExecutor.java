/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.monitor.executors;

import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.RunnableProcess;
import org.opencb.opencga.catalog.models.Job;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 22/08/16.
 */
public class LocalExecutor extends AbstractExecutor {

    private static int threadInitNumber;

    public LocalExecutor() {
        logger = LoggerFactory.getLogger(LocalExecutor.class);
    }

    @Override
    public void execute(Job job) throws Exception {
        Runnable runnable = () -> {
            ExecutorConfig executorConfig = null;
            try {
                executorConfig = getExecutorConfig(job);

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
                logger.error("Could not create the output/error files", e);
            } finally {
                if (executorConfig != null) {
                    Path outdir = Paths.get(executorConfig.getOutdir());
                    // The outdir folder may be removed by the IndexDaemon
                    if (outdir.toFile().exists()) {
                        String status = status(outdir, job);
                        if (!status.equals(Job.JobStatus.DONE)
                                && !status.equals(Job.JobStatus.READY)
                                && !status.equals(Job.JobStatus.ERROR)) {
                            logger.error("Job {} finished with status {}. Write {} with status {}",
                                    job.getId(), status, JOB_STATUS_FILE, Job.JobStatus.ERROR);
                            try {
                                Path jobStatusFile = outdir.resolve(JOB_STATUS_FILE);
                                Job.JobStatus jobStatus = new Job.JobStatus(Job.JobStatus.ERROR, "Job finished with status " + status);
                                objectMapper.writer().writeValue(jobStatusFile.toFile(), jobStatus);
                            } catch (IOException e) {
                                logger.error("Could not write the " + JOB_STATUS_FILE + " with status " + Job.JobStatus.ERROR, e);
                            }
                        }
                    }
                }
            }
        };
        Thread thread = new Thread(runnable, "LocalExecutor-" + nextThreadNum());
        thread.start();
    }

    private static synchronized int nextThreadNum() {
        return threadInitNumber++;
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
