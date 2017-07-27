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

package org.opencb.opencga.core.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class has been moved to java-common-libs.
 */
@Deprecated
public class SingleProcess {

    private Logger logger;

    private RunnableProcess runnableProcess;

    private ExecutorService execSvc;

    private int timeout;


    public SingleProcess() {
        logger = LoggerFactory.getLogger(this.getClass());
        timeout = 0;
    }

    public SingleProcess(RunnableProcess obj) {
        this();
        this.setRunnableProcess(obj);
        getRunnableProcess().setStatus(RunnableProcess.Status.WAITING);
    }

    public void runSync() {
        execSvc = Executors.newSingleThreadExecutor();
        execSvc.execute(getRunnableProcess());
        execSvc.shutdown();
        waitFor();
    }

    public void runAsync() {
        execSvc = Executors.newSingleThreadExecutor();
        execSvc.execute(getRunnableProcess());
        execSvc.shutdown();
    }

    public void kill() {
        if (!execSvc.isTerminated()) {
            getRunnableProcess().setStatus(RunnableProcess.Status.KILLED);
            runnableProcess.destroy();
            execSvc.shutdownNow();
            getRunnableProcess().setEndTime(System.currentTimeMillis());
            getRunnableProcess().setExitValue(-1);
            getRunnableProcess().setError("Aborted by the user");
        }
    }

    public void waitFor() {
        long currentTime = 0;
        try {
            while (!execSvc.isTerminated()) {
                Thread.sleep(500);
                if (getTimeout() > 0 && currentTime > getTimeout()) {
                    kill();
                    getRunnableProcess().setStatus(RunnableProcess.Status.TIMEOUT);
                    getRunnableProcess().setError("Timeout error");
                    break;
                }
                currentTime = System.currentTimeMillis() - getRunnableProcess().getStartTime();
            }
        } catch (InterruptedException e) {
            logger.error(e.toString());
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Execution time: ").append(getRunnableProcess().getDuration()).append(System.getProperty("line.separator"));
        sb.append("Output: ").append(getRunnableProcess().getOutput()).append(System.getProperty("line.separator"));
        sb.append("Error: ").append(getRunnableProcess().getError()).append(System.getProperty("line.separator"));
        sb.append("ExitValue: ").append(getRunnableProcess().getExitValue()).append(System.getProperty("line.separator"));
        sb.append("Status: ").append(getRunnableProcess().getStatus()).append(System.getProperty("line.separator"));
        return sb.toString();
    }

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param runnableProcess the runnableProcess to set
     */
    public void setRunnableProcess(RunnableProcess runnableProcess) {
        this.runnableProcess = runnableProcess;
    }

    /**
     * @return the runnableProcess
     */
    public RunnableProcess getRunnableProcess() {
        return runnableProcess;
    }

}
