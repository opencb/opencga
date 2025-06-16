package org.opencb.opencga.storage.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Watchdog extends Thread {

    private static final AtomicInteger WATCHDOG_COUNTER = new AtomicInteger(0);
    protected final long timeoutMillis;
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Logger logger = LoggerFactory.getLogger(Watchdog.class);
    private final Thread hook;

    public Watchdog(String name) {
        this(name, 1, TimeUnit.MINUTES);
    }

    public Watchdog(String name, long timeout, TimeUnit timeUnit) {
        this.timeoutMillis = timeUnit.toMillis(timeout);
        this.setName(name + "-" + WATCHDOG_COUNTER.incrementAndGet());
        // Set the thread to be a daemon thread so it does not block JVM shutdown
        this.setDaemon(true);
        hook = new Thread(() -> {
            active.set(false);
        });
    }

    public void stopWatchdog() {
        active.set(false);
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (Exception e) {
            // Should not happen, but log it just in case
            logger.warn("Error removing shutdown hook for Watchdog", e);
        }
    }

    @Override
    public final void run() {
        Runtime.getRuntime().addShutdownHook(hook);

        while (active.get()) {
            try {
                updateStatus();
            } catch (Exception e) {
                // Log the exception, but do not stop the watchdog
                logger.error("Error updating status in IsLoadingWatchdog", e);
            }
            try {
                Thread.sleep(timeoutMillis);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        Runtime.getRuntime().removeShutdownHook(hook);
        try {
            onShutdown();
        } catch (Exception e) {
            logger.error("Error updating status in IsLoadingWatchdog on shutdown", e);
        }
    }

    protected abstract void updateStatus() throws Exception;

    protected void onShutdown() throws Exception{

    }
}
