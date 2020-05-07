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

import org.opencb.opencga.core.config.Execution;
import org.opencb.opencga.core.models.common.Enums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Created by pfurio on 24/08/16.
 */
public class SGEExecutor implements BatchExecutor {

    private SGEManager sgeManager;
    private static Logger logger;

    public SGEExecutor(Execution execution) {
        logger = LoggerFactory.getLogger(SGEExecutor.class);
        sgeManager = new SGEManager(execution);
    }

    @Override
    public void execute(String jobId, String commandLine, Path stdout, Path stderr) throws Exception {
        sgeManager.queueJob(jobId, "", -1, getCommandLine(commandLine, stdout, stderr), null);
    }

    @Override
    public String getStatus(String jobId) {
        return Enums.ExecutionStatus.UNKNOWN;
//        String status;
//        try {
//            status = SgeManager.status(Objects.toString(job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME)));
//        } catch (Exception e) {
//            logger.error("Could not obtain the status of the job {} from SGE. Error: {}", job.getUid(), e.getMessage());
//            return Job.JobStatus.UNKNOWN;
//        }
//
//        switch (status) {
//            case SgeManager.ERROR:
//            case SgeManager.EXECUTION_ERROR:
//                return Job.JobStatus.ERROR;
//            case SgeManager.FINISHED:
//                return Job.JobStatus.READY;
//            case SgeManager.QUEUED:
//                return Job.JobStatus.QUEUED;
//            case SgeManager.RUNNING:
//            case SgeManager.TRANSFERRED:
//                return Job.JobStatus.RUNNING;
//            case SgeManager.UNKNOWN:
//            default:
//                return job.getStatus().getName();
//        }
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
    public boolean isExecutorAlive() {
        return false;
    }
}
