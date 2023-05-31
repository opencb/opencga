/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created on 18/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class HBaseLockManagerTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private HBaseLockManager hbaseLock;

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        HBaseManager hbaseManager = new HBaseManager(configuration.get());
        hbaseManager.createTableIfNeeded(DB_NAME, Bytes.toBytes("0"), Compression.Algorithm.NONE);
        hbaseLock = new HBaseLockManager(hbaseManager, DB_NAME, Bytes.toBytes("0"), Bytes.toBytes("R"));

        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) HBaseLockManager.buildThreadPool();
        HBaseLockManager.threadPool = threadPool;
        assertEquals(0, threadPool.getActiveCount());
        assertEquals(0, threadPool.getPoolSize());
    }

    @After
    public void tearDown() throws Exception {
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) HBaseLockManager.threadPool;
        if (!thrown.isAnyExceptionExpected()) {
            assertEquals(0, threadPool.getActiveCount());
        }
    }

    @Test
    public void testLock() throws Exception {
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) HBaseLockManager.threadPool;
        int lockId = 1;
        for (int i = 0; i < 10; i++) {
            int millis = 248 + (i % 13);
            System.out.println("i = " + i + " " + millis + "ms");
            Lock lock = hbaseLock.lock(getColumn(lockId), 1000, 1000);
            Thread.sleep(millis);
            System.out.println("lock = " + lock);
            lock.unlock();
        }
        assertEquals(1, threadPool.getPoolSize());
        assertEquals(0, threadPool.getActiveCount());
    }

    @Test
    public void testConcurrentLock() throws Exception {
        int lockId = 2;

        AtomicInteger counter = new AtomicInteger(0);
        Set<String> threadWithLock = Collections.synchronizedSet(new HashSet<>());

        int nThreads = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        List<Future> futures = new ArrayList<>();
        for (int t = 0; t < nThreads; t++) {
            futures.add(executorService.submit(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        System.out.println("i = " + i);
                        Lock lock = hbaseLock.lock(getColumn(lockId), 1000, 20000);
                        System.out.println("[" + Thread.currentThread().getName() + "] Enter LOCK");
                        assertEquals(threadWithLock.toString(), 0, threadWithLock.size());
                        threadWithLock.add(Thread.currentThread().getName());
                        assertEquals(threadWithLock.toString(), 1, threadWithLock.size());
                        int value = counter.addAndGet(1);
                        Thread.sleep(100);
                        assertEquals(threadWithLock.toString(), 1, threadWithLock.size());
                        assertEquals(threadWithLock.toString(), value, counter.get());
                        threadWithLock.remove(Thread.currentThread().getName());
                        System.out.println("lock = " + lock);
                        System.out.println("[" + Thread.currentThread().getName() + "] Exit LOCK");
                        lock.unlock();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        executorService.shutdown();
        executorService.awaitTermination(2000, TimeUnit.SECONDS);

        for (Future future : futures) {
            assertTrue(future.isDone());
            future.get();
        }

    }

    public byte[] getColumn(int lockId) {
        return Bytes.toBytes(lockId);
    }

    @Test
    public void testLockAndLock() throws Exception {
        int lockId = 3;
        Lock lock = hbaseLock.lock(getColumn(lockId), 1000, 2000);
        System.out.println("lock = " + lock);

        thrown.expect(TimeoutException.class);
        hbaseLock.lock(getColumn(lockId), 1000, 1000);

    }

    @Test
    public void testLockAfterExpiring() throws Exception {
        int lockId = 4;
        Lock lock = hbaseLock.lock(getColumn(lockId), 1000, 1000);
        lock.keepAliveStop();
        System.out.println("lock = " + lock);

        Thread.sleep(2000);
        System.out.println("Expired lock = " + lock);

        lock = hbaseLock.lock(getColumn(lockId), 1000, 1000);
        System.out.println("Unlock = " + lock);
        lock.unlock();
    }

    @Test
    public void testLockExpireUnlock() throws Exception {
        Lock lock = hbaseLock.lock(getColumn(1), 100, 100);
        lock.keepAliveStop();
        System.out.println("lock = " + lock);
        Thread.sleep(200);

        thrown.expect(HBaseLockManager.IllegalLockStatusException.class);
        lock.unlock();
    }

    @Test
    public void testLockAndRefresh() throws Exception {
        Lock lock = hbaseLock.lock(getColumn(1), 100, 100);
        lock.keepAliveStop();
        System.out.println("lock = " + lock);

        for (int i = 0; i < 10; i++) {
            Thread.sleep(50); // small delay
            lock.refresh();
        }

        lock.unlock();
    }

    @Test
    public void testLockRefreshExpireUnlock() throws Exception {
        Lock lock = hbaseLock.lock(getColumn(1), 100, 100);
        lock.keepAliveStop();
        System.out.println("lock = " + lock);
        lock.refresh();

        Thread.sleep(200); // expire

        thrown.expect(HBaseLockManager.IllegalLockStatusException.class);
        lock.unlock();
    }

    @Test
    public void testLockRefreshExpiredRefresh() throws Exception {
        Lock lock = hbaseLock.lock(getColumn(1), 100, 100);
        lock.keepAliveStop();
        System.out.println("lock = " + lock);
        lock.keepAliveStop();

        Thread.sleep(200); // expire

        thrown.expect(HBaseLockManager.IllegalLockStatusException.class);
        lock.refresh();
    }

    @Test
    public void testGetCurrent() {
        long e = System.currentTimeMillis() + 1000;
        String s;

        // Expired current token
        s = HBaseLockManager.parseValidLockToken(Bytes.toBytes("CURRENT-abc:123"));
        assertNull(s);

        // Valid current token
        s = HBaseLockManager.parseValidLockToken(Bytes.toBytes("CURRENT-abc:" + e));
        assertEquals("abc", s);

        // Current expired, first refresh valid
        s = HBaseLockManager.parseValidLockToken(Bytes.toBytes("REFRESH-abc:" + e));
        assertEquals("abc", s);

        // Expired refresh
        s = HBaseLockManager.parseValidLockToken(Bytes.toBytes("REFRESH-abc:200"));
        assertNull(s);
    }

}