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

import org.opencb.opencga.core.models.job.Job;

/**
 * Created by pfurio on 22/08/16.
 */
public class ExecutorConfig {

    private String stdout;
    private String stderr;
    private String outdir;
    private int timeout;   // seconds
    private int numThreads;
    private int maxMem;    // MB

    public ExecutorConfig() {
        this("/tmp/stdout.txt", "/tmp/stderr.txt", "/tmp/", 3600, 1, 1024);
    }

    public ExecutorConfig(String stdout, String stderr, String outdir, int timeout, int numThreads, int maxMem) {
        this.stdout = stdout;
        this.stderr = stderr;
        this.outdir = outdir;
        this.timeout = timeout;
        this.numThreads = numThreads;
        this.maxMem = maxMem;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutorConfig{");
        sb.append("stdout='").append(stdout).append('\'');
        sb.append(", stderr='").append(stderr).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", numThreads=").append(numThreads);
        sb.append(", maxMem=").append(maxMem);
        sb.append('}');
        return sb.toString();
    }

    public String getStdout() {
        return stdout;
    }

    public ExecutorConfig setStdout(String stdout) {
        this.stdout = stdout;
        return this;
    }

    public String getStderr() {
        return stderr;
    }

    public ExecutorConfig setStderr(String stderr) {
        this.stderr = stderr;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public ExecutorConfig setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public ExecutorConfig setNumThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    public int getMaxMem() {
        return maxMem;
    }

    public ExecutorConfig setMaxMem(int maxMem) {
        this.maxMem = maxMem;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public ExecutorConfig setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public static ExecutorConfig getExecutorConfig(Job job) {
        ExecutorConfig executorConfig = null;

//        if (job != null && job.getResourceManagerAttributes() != null) {
//            executorConfig = new ExecutorConfig();
//
//            if (job.getResourceManagerAttributes().get(BatchExecutor.STDOUT) != null) {
//                executorConfig.setStdout(job.getResourceManagerAttributes().get(BatchExecutor.STDOUT).toString());
//            }
//
//            if (job.getResourceManagerAttributes().get(BatchExecutor.STDERR) != null) {
//                executorConfig.setStderr(job.getResourceManagerAttributes().get(BatchExecutor.STDERR).toString());
//            }
//
//            if (job.getResourceManagerAttributes().get(BatchExecutor.OUTDIR) != null) {
//                executorConfig.setOutdir(job.getResourceManagerAttributes().get(BatchExecutor.OUTDIR).toString());
//            }
//
//            if (job.getResourceManagerAttributes().get(BatchExecutor.TIMEOUT) != null) {
//                executorConfig.setTimeout(Integer.parseInt(job.getResourceManagerAttributes().get(BatchExecutor.TIMEOUT).toString()));
//            }
//
//            if (job.getResourceManagerAttributes().get(BatchExecutor.MAX_MEM) != null) {
//                executorConfig.setMaxMem(Integer.parseInt(job.getResourceManagerAttributes().get(BatchExecutor.MAX_MEM).toString()));
//            }
//
//            if (job.getResourceManagerAttributes().get(BatchExecutor.NUM_THREADS) != null) {
//                executorConfig.setNumThreads(Integer.parseInt(job.getResourceManagerAttributes()
//                        .get(BatchExecutor.NUM_THREADS).toString()));
//            }
//        }

        return executorConfig;
    }
}
