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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread for monitoring the memory usage of the JDK.
 * <p>
 * Usage:
 * MemoryUsageMonitor m = new MemoryUsageMonitor().start();
 * .....
 * m.stop();
 * <p>
 * Created on 11/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MemoryUsageMonitor {

    private static final double MEGABYTE = 1024 * 1024;
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final Logger logger;
    private int delayMillis;
    private Thread thread;

    public MemoryUsageMonitor() {
        this.thread = new Thread(this::run);
        this.thread.setName("memory-monitor");
        this.logger = LoggerFactory.getLogger(MemoryUsageMonitor.class);
        delayMillis = 5000;
    }

    public int getDelay() {
        return delayMillis;
    }

    public MemoryUsageMonitor setDelay(int delay, TimeUnit timeUnit) {
        this.delayMillis = (int) timeUnit.toMillis(delay);
        return this;
    }

    public synchronized MemoryUsageMonitor start() {
        if (!active.get()) {
            active.set(true);
            thread.start();
        }
        return this;
    }

    public synchronized MemoryUsageMonitor stop() {
        if (active.get()) {
            active.set(false);
            this.thread.interrupt();
            try {
                this.thread.join();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
            logMemory(logger);
        }
        return this;
    }

    private void run() {
        try {
            while (active.get()) {
                logMemory();
                Thread.sleep(delayMillis);
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
        logger.info(String.format("Memory usage. MaxMemory: %.2f MiB"
                        + " TotalMemory: %.2f MiB"
                        + " FreeMemory: %.2f MiB"
                        + " UsedMemory: %.2f MiB", rt.maxMemory() / MEGABYTE, rt.totalMemory() / MEGABYTE,
                rt.freeMemory() / MEGABYTE, (rt.totalMemory() - rt.freeMemory()) / MEGABYTE));
    }

}
