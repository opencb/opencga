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
    private final Object lock = new Object();

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
        if (active.get()) {
            synchronized (lock) {
                active.set(false);
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                } catch (Exception e) {
                    // Should not happen, but log it just in case
                    logger.warn("Error removing shutdown hook for Watchdog", e);
                }
                try {
                    onShutdown();
                } catch (Exception e) {
                    logger.error("Error updating status in Watchdog on shutdown", e);
                }
            }
        }
    }

    @Override
    public final void run() {
        Runtime.getRuntime().addShutdownHook(hook);

        while (active.get()) {
            try {
                // Synchronize to avoid concurrent calls to updateStatus and stopWatchdog
                synchronized (lock) {
                    // Check if the watchdog is still active before updating status
                    if (!active.get()) {
                        break;
                    }
                    updateStatus();
                }
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
    }

    protected abstract void updateStatus() throws Exception;

    protected abstract void onShutdown() throws Exception;
}
