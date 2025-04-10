package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.opencga.storage.core.metadata.models.Lock;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DummyLock extends Lock  {

    protected static final Map<String, DummyLock> LOCKS = new ConcurrentHashMap<>();
    private final ReentrantLock lock;
    private final String key;

    interface LockStatement <E extends Exception, T> {
        T run(Lock lock) throws E;
    }

    public static <E extends Exception, T> T lock(int studyId, String resource, int resourceId, LockStatement<E, T> callback) throws E {
        try (Lock lock = getLock(studyId, resource, resourceId)) {
            return callback.run(lock);
        }
    }

    public static Lock getLock(int studyId, String resource, int resourceId) {
        String key = "S_" + studyId + "_" + resource + "_" + resourceId;

        DummyLock dummyLock = LOCKS.computeIfAbsent(key, k -> new DummyLock(key));

        try {
            dummyLock.lock.tryLock(1000, TimeUnit.MILLISECONDS);
            dummyLock.locked = true;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return dummyLock;
    }

    private DummyLock(String key) {
        super(key.hashCode());
        this.key = key;
        lock = new ReentrantLock();
    }

    @Override
    protected void unlock0() {
        lock.unlock();
        locked = false;
    }

    @Override
    public void refresh() throws IOException {

    }
}
