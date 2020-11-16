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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Concurrent lock using an HBase cell.
 *
 * Basic algorithm:
 *
 * Lock:
 *      String token = Random();
 *      HBase.append(row, column, token);
 *      if (HBase.get(row, column).startsWith(token)) {
 *           // Win the token
 *           HBase.put(row, column, "CURRENT-{token}:{exiration_date})
 *           return TRUE;
 *      } else {
 *           // Token already taken
 *           return FALSE;
 *      }
 *
 * Refresh:
 *     HBase.append(row, column, "REFRESH-{token}:{new_expiration_date}")
 *
 *
 * Unlock:
 *      HBase.put(row, column, "");
 *
 * Created on 19/05/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseLockManager {

    private static final String LOCK_SEPARATOR = "_";
    private static final String LOCK_EXPIRING_DATE_SEPARATOR = ":";
    private static final String LOCK_PREFIX_SEPARATOR = "-";
    private static final String CURRENT = "CURRENT" + LOCK_PREFIX_SEPARATOR;
    private static final String REFRESH = "REFRESH" + LOCK_PREFIX_SEPARATOR;
    protected static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("hbase-lock-%d")
                    .build());

    protected final HBaseManager hbaseManager;
    protected final String tableName;
    protected final byte[] columnFamily;
    protected final byte[] defaultRow;
    protected static Logger logger = LoggerFactory.getLogger(HBaseLockManager.class);

    public HBaseLockManager(HBaseManager hbaseManager, String tableName, byte[] columnFamily, byte[] row) {
        this.hbaseManager = hbaseManager;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.defaultRow = row;
    }

    /**
     * Apply for the lock.
     *
     * @param column        Column to find the lock cell
     * @param lockDuration  Duration un milliseconds of the token. After this time the token is expired.
     * @param timeout       Max time in milliseconds to wait for the lock
     *
     * @return              Lock token
     *
     * @throws InterruptedException if any thread has interrupted the current thread.
     * @throws TimeoutException if the operations takes more than the timeout value.
     * @throws IOException      if there is an error writing or reading from HBase.
     */
    public Lock lock(byte[] column, long lockDuration, long timeout)
            throws InterruptedException, TimeoutException, IOException {
        return lock(defaultRow, column, lockDuration, timeout);
    }

    /**
     * Apply for the lock.
     *
     * @param row           Row to find the lock cell
     * @param column        Column to find the lock cell
     * @param lockDuration  Duration un milliseconds of the token. After this time the token is expired.
     * @param timeout       Max time in milliseconds to wait for the lock
     *
     * @return              Lock token
     *
     * @throws InterruptedException if any thread has interrupted the current thread.
     * @throws TimeoutException if the operations takes more than the timeout value.
     * @throws IOException      if there is an error writing or reading from HBase.
     */
    public Lock lock(byte[] row, byte[] column, long lockDuration, long timeout)
            throws InterruptedException, TimeoutException, IOException {
        String token = RandomStringUtils.randomAlphanumeric(10);

        // Minimum lock duration of 100ms
        lockDuration = Math.max(lockDuration, 100);
        // Max attempt lock duration of 10s
        long attemptLockDuration = Math.min(lockDuration, 10000);

        String[] lockValue;
        String readToken = "";
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        lockValue = readLockValue(row, column);
        do {
            // If the lock is taken, wait
            while (isLockTaken(lockValue) || containsToken(lockValue, token)) {
                Thread.sleep(100);
                lockValue = readLockValue(row, column);
                //Check if the lock is still valid
                if (stopWatch.getTime() > timeout) {
                    throw new TimeoutException("Unable to get the lock");
                }
            }
            //Check if the lock is still valid
            if (stopWatch.getTime() > timeout) {
                throw new TimeoutException("Unable to get the lock");
            }

            // Append token to the lock cell
            appendToken(token, attemptLockDuration, row, column);

            lockValue = readLockValue(row, column);

            // Get the first non expired lock
            for (String lock : lockValue) {
                if (!isLockExpired(lock)) {
                    readToken = readLockToken(lock);
                    break;
                }
            }

            // You win the lock if the first available lock is yours.
        } while (!readToken.equals(token));

        if (stopWatch.getTime(TimeUnit.SECONDS) > 60) {
            logger.warn("Slow HBase lock for column '" + Bytes.toStringBinary(column) + "': " + TimeUtils.durationToString(stopWatch));
        }
        logger.debug("Won the lock with token " + token + " (" + token.hashCode() + ") from lock: " + Arrays.toString(lockValue));
        // Overwrite the lock with the winner current lock. Remove previous expired locks
        putCurrentLock(token, lockDuration, row, column);

        long tokenHash = token.hashCode();
        long finalLockDuration = lockDuration;
        return new Lock(THREAD_POOL, (int) (finalLockDuration / 4), tokenHash) {
            @Override
            public void unlock0() {
                try {
                    HBaseLockManager.this.unlock(row, column, tokenHash);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public synchronized void refresh() throws IOException {
                HBaseLockManager.this.refresh(row, column, tokenHash, finalLockDuration);
            }
        };
    }

    /**
     * Refreshes the lock.
     *
     * @param column        Column to find the lock cell
     * @param lockToken     Lock token
     * @param lockDuration  Duration un milliseconds of the token. After this time the token is expired.
     * @throws IOException      if there is an error writing or reading from HBase.
     */
    public void refresh(byte[] column, long lockToken, int lockDuration) throws IOException {
        refresh(defaultRow, column, lockToken, lockDuration);
    }


    /**
     * Refreshes the lock.
     *
     * @param row           Row to find the lock cell
     * @param column        Column to find the lock cell
     * @param lockToken     Lock token
     * @param lockDuration  Duration un milliseconds of the token. After this time the token is expired.
     * @throws IOException      if there is an error writing or reading from HBase.
     */
    public void refresh(byte[] row, byte[] column, long lockToken, long lockDuration) throws IOException {
        // Check token is valid
        String[] lockValue = readLockValue(row, column);
        String currentLockToken = getCurrentLockToken(lockValue);
        if (currentLockToken == null || currentLockToken.hashCode() != lockToken) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockToken, currentLockToken, lockValue);
        }

        // Append lock refresh
        HBaseManager.act(getConnection(), tableName, table -> {
            Append a = new Append(row);
            a.add(columnFamily, column,
                    Bytes.toBytes(
                            REFRESH + currentLockToken
                                    + LOCK_EXPIRING_DATE_SEPARATOR
                                    + (System.currentTimeMillis() + lockDuration)
                                    + LOCK_SEPARATOR));
            table.append(a);
        });

        // Check valid lock refresh
        lockValue = readLockValue(row, column);
        String newLockToken = getCurrentLockToken(lockValue);
        if (newLockToken == null || !newLockToken.equals(currentLockToken)) {
            logger.error("Current lock token:" + currentLockToken);
            logger.error("New lock token: " + newLockToken);
            throw IllegalLockStatusException.inconsistentLock(row, column, lockToken, currentLockToken, lockValue);
        }
    }

    /**
     * Releases the lock.
     *
     * @param column    Column to find the lock cell
     * @param lockToken Lock token
     * @throws IOException                if there is an error writing or reading from HBase.
     * @throws IllegalLockStatusException if the lockToken does not match with the current lockToken
     */
    public void unlock(byte[] column, long lockToken) throws IOException, IllegalLockStatusException {
        unlock(defaultRow, column, lockToken);
    }

    /**
     * Releases the lock.
     *
     * @param row       Row to find the lock cell
     * @param column    Column to find the lock cell
     * @param lockToken Lock token
     * @throws IOException                if there is an error writing or reading from HBase.
     * @throws IllegalLockStatusException if the lockToken does not match with the current lockToken
     */
    public void unlock(byte[] row, byte[] column, long lockToken) throws IOException, IllegalLockStatusException {
        String[] lockValue = readLockValue(row, column);

        String currentToken = getCurrentLockToken(lockValue);

        if (currentToken == null || currentToken.hashCode() != lockToken) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockToken, currentToken, lockValue);
        }

        logger.debug("Unlock lock with token " + lockToken);
        clearLock(row, column);
    }

    private void appendToken(String token, long lockDuration, byte[] row, byte[] qualifier) throws IOException {
        HBaseManager.act(getConnection(), tableName, table -> {
            Append a = new Append(row);
            byte[] columnFamily = getColumnFamily();

            a.add(columnFamily, qualifier,
                    Bytes.toBytes(
                            token
                            + LOCK_EXPIRING_DATE_SEPARATOR
                            + (System.currentTimeMillis() + lockDuration)
                            + LOCK_SEPARATOR));
            table.append(a);
        });
    }

    private void putCurrentLock(String token, long lockDuration, byte[] row, byte[] qualifier) throws IOException {
        HBaseManager.act(getConnection(), tableName, table -> {
            Put p = new Put(row);
            byte[] columnFamily = getColumnFamily();

            p.addColumn(columnFamily, qualifier,
                    Bytes.toBytes(
                            CURRENT
                            + token
                            + LOCK_EXPIRING_DATE_SEPARATOR
                            + (System.currentTimeMillis() + lockDuration)
                            + LOCK_SEPARATOR));
            table.put(p);
        });
    }

    private void clearLock(byte[] row, byte[] qualifier) throws IOException {
        HBaseManager.act(getConnection(), tableName, table -> {
            Put p = new Put(row);
            byte[] columnFamily = getColumnFamily();

            p.addColumn(columnFamily, qualifier, Bytes.toBytes(""));
            table.put(p);
        });
    }

    /**
     * Get current lock token.
     * @param lockValue lock values
     * @return Current lock token, if any
     */
    protected static String getCurrentLockToken(String[] lockValue) {
        String currentToken = null;
        for (String lock : lockValue) {
            if (lock.startsWith(CURRENT)) {
                currentToken = readLockToken(lock);
                if (!isLockExpired(lock)) {
                    return currentToken;
                }
                // Current token is lock.
                // Look for refresh locks
            } else if (currentToken != null && lock.startsWith(REFRESH)) {
                // Only check REFRESH locks if there is a current token
                if (!isLockExpired(lock)) {
                    if (readLockToken(lock).equals(currentToken)) {
                        // Lock tokens matches. Token was refreshed
                        return currentToken;
                    }
                }
            } else {
                // Either this lock entry is not a CURRENT or REFRESH entry, or the first entry was not a CURRENT one
                return null;
            }
        }
        return null;
    }

    /**
     * A lock is taken if there is any lockValue in the array, and
     * the token has not expired.
     *
     *
     * @param lockValue lock values
     * @return if the lock is taken
     */
    protected static boolean isLockTaken(String[] lockValue) {
        return getCurrentLockToken(lockValue) != null;
    }

    private static boolean containsToken(String[] lockValue, String token) {
        for (String lock : lockValue) {
            if (readLockToken(lock).equals(token) && !isLockExpired(lock)) {
                return true;
            }
        }
        return false;
    }

    protected static boolean isLockExpired(String lock) {
        long expireDate = readExpireDate(lock);
        return expireDate < System.currentTimeMillis();
    }

    protected static String readLockToken(String lock) {
        int idx1 = lock.indexOf(LOCK_PREFIX_SEPARATOR);
        int idx2 = lock.indexOf(LOCK_EXPIRING_DATE_SEPARATOR);
        return lock.substring(idx1 + 1, idx2);
    }

    private static long readExpireDate(String lock) {
        int i = lock.indexOf(LOCK_EXPIRING_DATE_SEPARATOR);
        return Long.parseLong(lock.substring(i + 1));
    }

    private String[] readLockValue(byte[] row, byte[] qualifier) throws IOException {
        String lockValue;
        lockValue = HBaseManager.act(getConnection(), tableName, table -> {
            byte[] columnFamily = getColumnFamily();

            Result result = table.get(new Get(row).addColumn(columnFamily, qualifier));
            if (result.isEmpty()) {
                return null;
            } else {
                return Bytes.toString(result.getValue(columnFamily, qualifier));
            }
        });

        if (lockValue == null || lockValue.isEmpty()) {
            return new String[0];
        } else {
            return lockValue.split(LOCK_SEPARATOR);
        }
    }

    private byte[] getColumnFamily() {
        return columnFamily;
    }

    private Connection getConnection() {
        return hbaseManager.getConnection();
    }

    public static class IllegalLockStatusException extends IllegalStateException {
        public IllegalLockStatusException(String s) {
            super(s);
        }

        public static IllegalLockStatusException inconsistentLock(byte[] row, byte[] column, long lockToken, String currentLock,
                                                                  String[] lockValue) {
            if (StringUtils.isEmpty(currentLock)) {
                String msg = "";
                if (lockValue.length > 0 && lockValue[0].startsWith(CURRENT)) {
                    if (readLockToken(lockValue[0]).hashCode() == lockToken) {
                        // Expired token
                        long millis = System.currentTimeMillis() - readExpireDate(lockValue[0]);
                        msg = "Current lock '" + lockValue[0] + "' expired " + millis + "ms ago.";
                        for (int i = lockValue.length - 1; i >= 0; i--) {
                            if (lockValue[i].startsWith(REFRESH) && readLockToken(lockValue[i]).hashCode() == lockToken) {
                                millis = System.currentTimeMillis() - readExpireDate(lockValue[i]);
                                msg += " Last refreshed lock '" + lockValue[i] + "' expired " + millis + "ms ago.";
                                break;
                            }
                        }
                    }
                }
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + "Lock: " + Arrays.toString(lockValue) + ". " + msg);
            } else {
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + lockToken + " != " + currentLock.hashCode() + " from " + Arrays.toString(lockValue));
            }
        }
    }
}
