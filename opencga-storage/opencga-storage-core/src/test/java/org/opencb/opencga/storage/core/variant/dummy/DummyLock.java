package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DummyLock extends Lock  {

    protected static final Map<String, DummyLock> LOCKS = new ConcurrentHashMap<>();
    private final ReentrantLock lock;
    private final String key;
    private final Object lockObject = new Object();
    private static Logger logger = LoggerFactory.getLogger(DummyLock.class);
    protected final AtomicLong expirationDate = new AtomicLong();
//    private StackTraceElement[] stackTrace;
//    protected final AtomicReference<String> threadName = new AtomicReference<>();

    interface LockStatement <E extends Exception, T> {
        T run(Lock lock) throws E;
    }

    public static <E extends Exception, T> T lock(int studyId, String resource, int resourceId, LockStatement<E, T> callback) throws E {
        try (Lock lock = getLock(studyId, resource, resourceId, 5000, 1000)) {
            return callback.run(lock);
        }
    }

    public static Lock getLock(int studyId, String resource, int resourceId, long lockDuration, long timeout) {
        String key = "S_" + studyId + "_" + resource + "_" + resourceId;

        DummyLock dummyLock = LOCKS.computeIfAbsent(key, k -> new DummyLock(key));

        int startTime = (int) System.currentTimeMillis();
        int time = 0;
        while (time < timeout) {
            try {
                dummyLock.checkExpired();

                if (dummyLock.tryLock((int) lockDuration, 100, TimeUnit.MILLISECONDS)) {
                    time = (int) (System.currentTimeMillis() - startTime);
                    if (time > 300) {
                        logger.info("Lock '" + key + "' acquired after " + time + " ms");
                    }
                    return dummyLock;
                } else {
                    time = (int) (System.currentTimeMillis() - startTime);
                    if (time > 300) {
                        logger.info("Lock '" + key + "' not acquired after " + time + " ms . It will expire in "
                                + (dummyLock.expirationDate.get() - System.currentTimeMillis()) + " ms");
                    }
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Could not acquire lock for " + key);
    }

    private void checkExpired() throws InterruptedException {
        synchronized (lockObject) {
            if (isLockedAndExpired()) {
//                logger.info("Lock '" + key + "' expired. Current time : " + System.currentTimeMillis() + " > " + expirationDate);
//                logger.info("Was created by " + threadName + " at:");
//                for (StackTraceElement stackTraceElement : stackTrace) {
//                    if (stackTraceElement.getClassName().contains("org.opencb")) {
//                        logger.info("  " + stackTraceElement);
//                    }
//                }
//                logger.info("Lock '" + key + "' expired. Current stack trace:");
//                for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
//                    if (stackTraceElement.getClassName().contains("org.opencb")) {
//                        logger.info("  " + stackTraceElement);
//                    }
//                }
                throw new IllegalStateException("Lock '" + key + "' expired.");
            }
        }
    }

    private DummyLock(String key) {
        super(key.hashCode());
        this.key = key;
        lock = new ReentrantLock();
        locked = false;
    }

    @Override
    public boolean isLocked() {
        return lock.isLocked() && locked;
    }

    public boolean isLockedAndExpired() throws InterruptedException {
        return isLocked() && System.currentTimeMillis() > expirationDate.get();
    }

    private boolean tryLock(int lockDuration, int timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lock.tryLock(timeout, timeUnit)) {
            synchronized (lockObject) {
                expirationDate.set(System.currentTimeMillis() + lockDuration);
//                stackTrace = Thread.currentThread().getStackTrace();
//                threadName.set(Thread.currentThread().getName());
                locked = true;
            }
            return true;
        } else {
            Thread.yield();
            return false;
        }
    }

    @Override
    public void checkLocked() {
        if (!isLocked()) {
            throw new IllegalStateException("Lock '" + getToken() + "'is unlocked");
        }
    }

    @Override
    protected synchronized void unlock0() {
        synchronized (lockObject) {
            locked = false;
//            expirationDate.set(0);
//            stackTrace = null;
            lock.unlock();
        }
    }

    @Override
    public void refresh() throws IOException {
    }
}
