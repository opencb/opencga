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

import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.RunnableProcess;
import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by pfurio on 22/08/16.
 */
public class LocalExecutor implements BatchExecutor {

    public static final String MAX_CONCURRENT_JOBS = "local.maxConcurrentJobs";

    private static int threadInitNumber;
    private static Logger logger;
    private final ExecutorService threadPool;
    private final Map<String, String> jobStatus;
    private final int maxConcurrentJobs;

    public LocalExecutor(Execution execution) {
        logger = LoggerFactory.getLogger(LocalExecutor.class);
        maxConcurrentJobs = execution.getOptions().getInt(MAX_CONCURRENT_JOBS, 1);
        threadPool = Executors.newFixedThreadPool(maxConcurrentJobs);
        jobStatus = Collections.synchronizedMap(new LinkedHashMap<String, String>(1000) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > 1000;
            }
        });
    }

    @Override
    public void execute(String jobId, String queue, String commandLine, Path stdout, Path stderr) throws Exception {
        jobStatus.put(jobId, Enums.ExecutionStatus.QUEUED);
        Runnable runnable = () -> {
            try {
                Thread.currentThread().setName("LocalExecutor-" + nextThreadNum());
                logger.info("Ready to run - {}", commandLine);
                jobStatus.put(jobId, Enums.ExecutionStatus.RUNNING);
                Command com = new Command(commandLine);

                DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(stdout.toFile()));
                com.setOutputOutputStream(dataOutputStream);

                dataOutputStream = new DataOutputStream(new FileOutputStream(stderr.toFile()));
                com.setErrorOutputStream(dataOutputStream);

                Thread hook = new Thread(() -> {
                    logger.info("Running ShutdownHook. Job {id: " + jobId + "} has being aborted.");
                    com.setStatus(RunnableProcess.Status.KILLED);
                    com.setExitValue(-2);
                    closeOutputStreams(com);
                    jobStatus.put(jobId, Enums.ExecutionStatus.ERROR);
                });

                logger.info("==========================================");
                logger.info("Executing job {}", jobId);
                logger.debug("Executing commandLine {}", commandLine);
                logger.info("==========================================");
                System.err.println();

                try {
                    Runtime.getRuntime().addShutdownHook(hook);
                    com.run();
                } finally {
                    Runtime.getRuntime().removeShutdownHook(hook);
                    closeOutputStreams(com);
                }

                System.err.println();
                logger.info("==========================================");
                logger.info("Finished job {}", jobId);
                logger.info("==========================================");

                if (com.getStatus().equals(RunnableProcess.Status.DONE)) {
                    jobStatus.put(jobId, Enums.ExecutionStatus.DONE);
                } else {
                    jobStatus.put(jobId, Enums.ExecutionStatus.ERROR);
                }
            } catch (Throwable throwable) {
                logger.error("Error running job " + jobId, throwable);
                jobStatus.put(jobId, Enums.ExecutionStatus.ERROR);
            }
        };
        threadPool.submit(runnable);
    }

    private static synchronized int nextThreadNum() {
        return threadInitNumber++;
    }

    @Override
    public String getStatus(String jobId) {
        return jobStatus.getOrDefault(jobId, Enums.ExecutionStatus.UNKNOWN);
    }

    @Override
    public boolean stop(String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean resume(String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean kill(String jobId) throws Exception {
        return false;
    }

    @Override
    public boolean canBeQueued() {
        return jobStatus.values()
                .stream()
                .filter(s -> s.equals(Enums.ExecutionStatus.RUNNING) || s.equals(Enums.ExecutionStatus.QUEUED))
                .count() < maxConcurrentJobs;
    }

    @Override
    public boolean isExecutorAlive() {
        return true;
    }

    private void closeOutputStreams(Command command) {
        /** Close output streams **/
        if (command.getOutputOutputStream() != null) {
            try {
                command.getOutputOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            command.setOutputOutputStream(null);
            command.setOutput(null);
        }
        if (command.getErrorOutputStream() != null) {
            try {
                command.getErrorOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            command.setErrorOutputStream(null);
            command.setError(null);
        }
    }
}
