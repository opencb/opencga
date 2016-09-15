package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread for monitoring the memory usage of the JDK.
 *
 * Usage:
 *     MemoryUsageMonitor m = new MemoryUsageMonitor().start();
 *     .....
 *     m.interrupt();
 *
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
        } catch (InterruptedException ignore) {}
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
