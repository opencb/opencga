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

import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;
import java.nio.file.Path;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 22/08/16.
 */
public interface BatchExecutor {

    String TIMEOUT = "timeout";
    String STDOUT = "stdout";
    String STDERR = "stderr";
    String OUTDIR = "outdir";
    String NUM_THREADS = "num_threads";
    String MAX_MEM = "max_mem";
    String JOB_STATUS_FILE = "status.json";
    String OUT_LOG_EXTENSION = ".out";
    String ERR_LOG_EXTENSION = ".err";

    void execute(Job job, String token) throws Exception;

    String getStatus(Job job);

    boolean stop(Job job) throws Exception;

    boolean resume(Job job) throws Exception;

    boolean kill(Job job) throws Exception;

    boolean isExecutorAlive();

    /**
     * We do it this way to avoid writing the session id in the command line (avoid display/monitor/logs) attribute of Job.
     * @param job Job to generate CLI from
     * @param token A valid session token
     * @return The command line
     */
    default String getCommandLine(Job job, String token) {
        return job.getCommandLine() + " --session-id " + token;
    }

    default String status(Path jobOutput, Job job) {
        ObjectReader objectReader = getDefaultObjectMapper().reader(Job.JobStatus.class);
        Path jobStatusFilePath = jobOutput.resolve(JOB_STATUS_FILE);
        if (!jobStatusFilePath.toFile().exists()) {
            return getStatus(job);
        }
        // File exists
        try {
            Job.JobStatus jobStatus = objectReader.readValue(jobStatusFilePath.toFile());
            return jobStatus.getName();
        } catch (IOException e) {
            return getStatus(job);
        }
    }
}
