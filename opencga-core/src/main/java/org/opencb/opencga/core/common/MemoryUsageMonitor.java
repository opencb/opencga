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

package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread for monitoring the memory usage of the JDK.
 * <p>
 * Usage:
 * MemoryUsageMonitor m = new MemoryUsageMonitor().start();
 * .....
 * m.interrupt();
 * <p>
 * Created on 11/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MemoryUsageMonitor extends Thread {

    private boolean active = true;
    private final Logger logger;
    private int delay;

    public MemoryUsageMonitor(Logger logger) {
        this.logger = logger;
        delay = 1000;
    }

    public MemoryUsageMonitor() {
        this.setName("memory-monitor");
        this.logger = LoggerFactory.getLogger(MemoryUsageMonitor.class);
        delay = 1000;
    }

    public int getDelay() {
        return delay;
    }

    public MemoryUsageMonitor setDelay(int delay) {
        this.delay = delay;
        return this;
    }

    @Override
    public void run() {
        try {
            while (active) {
                logMemory();
                Thread.sleep(delay);
            }
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    public void logMemory() {
        logMemory(logger);
    }

    public static void logMemory(Logger logger) {
        Runtime rt = Runtime.getRuntime();
        //1048576 = 1024*1024 = 2^20
        logger.info(String.format("Memory usage. MaxMemory: %.2f MiB"
                        + " TotalMemory: %.2f MiB"
                        + " FreeMemory: %.2f MiB"
                        + " UsedMemory: %.2f MiB", rt.maxMemory() / 1048576.0, rt.totalMemory() / 1048576.0,
                rt.freeMemory() / 1048576.0, (rt.totalMemory() - rt.freeMemory()) / 1048576.0));
    }

}
