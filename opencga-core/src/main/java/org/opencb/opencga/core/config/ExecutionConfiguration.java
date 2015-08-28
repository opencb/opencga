/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.core.config;

/**
 * Created by imedina on 28/08/15.
 */
public class ExecutionConfiguration {

//    OPENCGA.ANALYSIS.LOG.LEVEL = 1
//    OPENCGA.ANALYSIS.BINARIES.PATH = tools
//    OPENCGA.ANALYSIS.JOB.EXECUTOR = ${OPENCGA.ANALYSIS.EXECUTION.MANAGER}
//    OPENCGA.SGE.LOG.LEVEL = 1
//    OPENCGA.SGE.AVAILABLE.QUEUES = ${OPENCGA.ANALYSIS.SGE.AVAILABLE.QUEUES}
//    OPENCGA.SGE.DEFAULT.QUEUE = ${OPENCGA.ANALYSIS.SGE.DEFAULT.QUEUE}
//    OPENCGA.SGE.NORMAL.Q.TOOLS = *
//    CELLBASE.HOST = ws.bionifo.cipf.es
//    CELLBASE.NAME = cellbase

    /**
     * Directory where all tools are installed
     */
    public String tools;

    /**
     * How job is executed, this is one of the following: thread (old local), SGE, SLURM
     */
    public String executor;
    public String executorQueueName;

    public int timeout;

    public ExecutionConfiguration() {
    }

    public ExecutionConfiguration(String executor, String tools) {
        this.executor = executor;
        this.tools = tools;
    }


    public String getTools() {
        return tools;
    }

    public void setTools(String tools) {
        this.tools = tools;
    }

    public String getExecutor() {
        return executor;
    }

    public void setExecutor(String executor) {
        this.executor = executor;
    }

    public String getExecutorQueueName() {
        return executorQueueName;
    }

    public void setExecutorQueueName(String executorQueueName) {
        this.executorQueueName = executorQueueName;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
