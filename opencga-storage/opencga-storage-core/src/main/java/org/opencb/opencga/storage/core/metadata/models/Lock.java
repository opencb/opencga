package org.opencb.opencga.storage.core.metadata.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reference to a locked element.
 *
 * Created on 05/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class Lock implements Closeable {
    protected static Logger logger = LoggerFactory.getLogger(Lock.class);

    private final AtomicLong token = new AtomicLong();
    private final AtomicBoolean keepAlive;
    private final AtomicReference<Exception> exception = new AtomicReference<>();
    private boolean locked;
    private Future<?> keepAliveFuture;

    public Lock(long token) {
        this.token.set(token);
        this.keepAlive = new AtomicBoolean(false);
        this.locked = true;
    }

    public Lock(ExecutorService executorService, int keepAliveIntervalMillis, long token) {
        this.token.set(token);
        this.keepAlive = new AtomicBoolean(true);
        this.locked = true;
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
                    exception.set(e);
                }
            }
        });
    }

    public long getToken() {
        return token.get();
    }

    protected Lock setToken(long token) {
        this.token.set(token);
        return this;
    }

    public void checkLocked() {
        try {
            if (!isLocked()) {
                locked = false;
                throw new IllegalStateException("Lock '" + getToken() + "'is unlocked");
            }
        } catch (IOException e) {
            locked = false;
            throw new UncheckedIOException(e);
        }
    }

    public boolean isLocked() throws IOException {
        if (exception.get() != null) {
            throw new IOException(exception.get());
        } else {
            if (locked) {
                refresh();
            }
            return locked;
        }
    }

    /**
     * Unlock the locked element.
     */
    public final void unlock() {
        if (locked) {
            keepAliveStop();
            unlock0();
            locked = false;
        }
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
     * @throws IOException if the token can not be refreshed.
     */
    public abstract void refresh() throws IOException;
}
