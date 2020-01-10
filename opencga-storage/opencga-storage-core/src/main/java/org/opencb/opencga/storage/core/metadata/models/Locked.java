package org.opencb.opencga.storage.core.metadata.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reference to a locked element.
 *
 * Created on 05/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class Locked implements Closeable {
    protected static Logger logger = LoggerFactory.getLogger(Locked.class);

    private final AtomicLong token = new AtomicLong();
    private final AtomicBoolean keepAlive;
    private Future<?> keepAliveFuture;

    public Locked(long token) {
        this.token.set(token);
        this.keepAlive = new AtomicBoolean(false);
    }

    public Locked(ExecutorService executorService, int keepAliveIntervalMillis, long token) {
        this.token.set(token);
        this.keepAlive = new AtomicBoolean(true);
        keepAliveFuture = executorService.submit(() -> {
            try {
                while (keepAlive.get()) {
                    Thread.sleep(keepAliveIntervalMillis);
                    if (keepAlive.get()) {
                        refresh();
                    }
                }
            } catch (Exception e) {
                if (!(e instanceof InterruptedException)) {
                    logger.error("Catch exception at Locked.keepAlive", e);
                }
            }
        });
    }

    public long getToken() {
        return token.get();
    }

    protected Locked setToken(long token) {
        this.token.set(token);
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
        if (keepAliveFuture != null) {
            keepAliveFuture.cancel(true);
        }
    }

    protected abstract void unlock0();

    /**
     * Refresh the locked element. Allow keep-alive thread.
     */
    public abstract void refresh();
}
