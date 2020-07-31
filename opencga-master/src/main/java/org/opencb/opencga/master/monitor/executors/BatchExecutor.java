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

import java.nio.file.Path;

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
    @Deprecated
    String JOB_STATUS_FILE = "status.json";
    String OUT_LOG_EXTENSION = ".out";
    String ERR_LOG_EXTENSION = ".err";

    void execute(String jobId, String queue, String commandLine, Path stdout, Path stderr) throws Exception;

    String getStatus(String jobId);

    boolean stop(String jobId) throws Exception;

    boolean resume(String jobId) throws Exception;

    boolean kill(String jobId) throws Exception;

    default boolean canBeQueued() {
        return true;
    }

    boolean isExecutorAlive();

    default String getCommandLine(String commandLine) {
        return getCommandLine(commandLine, null, null);
    }

    /**
     * We do it this way to avoid writing the session id in the command line (avoid display/monitor/logs) attribute of Job.
     * @param commandLine Basic command line
     * @param stdout File where the standard output will be redirected
     * @param stderr File where the standard error will be redirected
     * @return The complete command line
     */
    default String getCommandLine(String commandLine, Path stdout, Path stderr) {
        if (stderr != null) {
            commandLine = commandLine + " 2>> " + stderr.toString();
        }
        if (stdout != null) {
            commandLine = commandLine + " >> " + stdout.toString();
        }
        return commandLine;
    }
}
