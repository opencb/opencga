package org.opencb.opencga.storage.core.metadata.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reference to a locked element.
 *
 * Created on 05/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class Locked implements Closeable {
    protected static Logger logger = LoggerFactory.getLogger(Locked.class);

    private long token;
    private final AtomicBoolean keepAlive;

    public Locked(long token) {
        this.token = token;
        this.keepAlive = new AtomicBoolean(false);
    }

    public Locked(ExecutorService executorService, int keepAliveIntervalMillis, long token) {
        this.token = token;
        this.keepAlive = new AtomicBoolean(true);
        executorService.submit(() -> {
            try {
                while (keepAlive.get()) {
                    Thread.sleep(keepAliveIntervalMillis);
                    if (keepAlive.get()) {
                        refresh();
                    }
                }
            } catch (Exception e) {
                logger.error("Catch exception at Locked.keepAlive", e);
            }
        });
    }

    public long getToken() {
        return token;
    }

    protected Locked setToken(long token) {
        this.token = token;
        return this;
    }

    /**
     * Unlock the locked element.
     */
    public final void unlock() {
        keepAliveStop();
        unlock0();
    }

    @Override
    public void close() {
        unlock();
    }

    public void keepAliveStop() {
        keepAlive.set(false);
    }

    protected abstract void unlock0();

    /**
     * Refresh the locked element. Allow keep-alive thread.
     */
    public abstract void refresh();
}
