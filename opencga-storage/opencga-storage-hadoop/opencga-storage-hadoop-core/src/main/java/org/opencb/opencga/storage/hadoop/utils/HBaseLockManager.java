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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Concurrent lock using an HBase cell.
 *
 * Basic algorithm:
 *
 * Lock:
 *      String token = Random();
 *      value = HBase.get(row, column)
 *      if (isNotTaken(value)) {
 *          if (HBase.checkAndPut(row, column, EQ, value, Put("CURRENT-{token}:{exiration_date}"))) {
 *             // Win the token
 *             return TRUE;
 *          } else {
 *              // Couldn't take the token
 *              return FALSE;
 *          }
 *      } else {
 *           // Token already taken
 *           return FALSE;
 *      }
 *
 * Refresh:
 *     value = HBase.get(row, column)
 *     HBase.checkAndPut(row, column, EQ, value, Put("REFRESH-{token}:{new_expiration_date}"))
 *
 *
 * Unlock:
 *     value = HBase.get(row, column)
 *     HBase.checkAndPut(row, column, EQ, value, Put(""))
 *
 * Created on 19/05/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseLockManager {

    private static final byte LOCK_EXPIRING_DATE_SEPARATOR_BYTE = ':';
    private static final String LOCK_EXPIRING_DATE_SEPARATOR_STR = ":";
    private static final byte LOCK_PREFIX_SEPARATOR_BYTE = '-';
    private static final String LOCK_PREFIX_SEPARATOR_STR = "-";
    private static final byte DEPRECATED_LOCK_SEPARATOR_BYTE = '_';
    private static final String CURRENT = "CURRENT" + LOCK_PREFIX_SEPARATOR_STR;
    private static final String REFRESH = "REFRESH" + LOCK_PREFIX_SEPARATOR_STR;
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

        byte[] lockValue;
        String readToken = "";
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        do {
            lockValue = readLockValue(row, column);

            // If the lock is taken, wait
            while (isLockTaken(lockValue)) {
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

            // Try to lock cell
            if (tryToPutToken(token, lockDuration, row, column, lockValue, CURRENT)) {
                readToken = parseValidLockToken(readLockValue(row, column));
            }

            // You win the lock if the first available lock is yours.
        } while (!token.equals(readToken));

        boolean prevTokenExpired = lockValue != null && lockValue.length > 0;
        boolean slowQuery = stopWatch.getTime() > 60000;
        if (prevTokenExpired || slowQuery) {
            StringBuilder msg = new StringBuilder("Lock column '").append(Bytes.toStringBinary(column)).append("'");
            if (prevTokenExpired) {
                long expireDate = parseExpireDate(lockValue);
                msg.append(". Previous token expired ")
                        .append(TimeUtils.durationToString(System.currentTimeMillis() - expireDate))
                        .append(" ago");
            }
            if (slowQuery) {
                msg.append(". Slow HBase lock");
            }
            msg.append(". Took: ").append(TimeUtils.durationToString(stopWatch));
            logger.warn(msg.toString());
        }

        long tokenHash = token.hashCode();
        logger.debug("Won the lock with token " + token + " (" + tokenHash + ")");

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
        byte[] lockValue = readLockValue(row, column);
        String currentLockToken = parseValidLockToken(lockValue);
        if (currentLockToken == null || currentLockToken.hashCode() != lockToken) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockToken, currentLockToken, lockValue);
        }

        if (!tryToPutToken(currentLockToken, lockDuration, row, column, lockValue, REFRESH)) {
            // Error refreshing!
            lockValue = readLockValue(row, column);
            String newLockToken = parseValidLockToken(lockValue);

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
        byte[] lockValue = readLockValue(row, column);

        String currentToken = parseValidLockToken(lockValue);

        if (currentToken == null || currentToken.hashCode() != lockToken) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockToken, currentToken, lockValue);
        }

        logger.debug("Unlock lock with token " + lockToken);
        if (!clearLock(row, column, lockValue)) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockToken, currentToken, lockValue);
        }
    }

    private Boolean tryToPutToken(String token, long lockDuration, byte[] row, byte[] qualifier, byte[] lockValue, String type)
            throws IOException {
        return HBaseManager.act(getConnection(), tableName, table -> {
            Put put = new Put(row)
                    .addColumn(columnFamily, qualifier, Bytes.toBytes(
                            type
                                    + token
                                    + LOCK_EXPIRING_DATE_SEPARATOR_STR
                                    + (System.currentTimeMillis() + lockDuration)));
            return table.checkAndPut(row, columnFamily, qualifier, CompareFilter.CompareOp.EQUAL, lockValue, put);
        });
    }

    private boolean clearLock(byte[] row, byte[] qualifier, byte[] lockValue) throws IOException {
        return HBaseManager.act(getConnection(), tableName, table -> {
            Put put = new Put(row)
                    .addColumn(columnFamily, qualifier, Bytes.toBytes(""));
            return table.checkAndPut(row, columnFamily, qualifier, CompareFilter.CompareOp.EQUAL, lockValue, put);
        });
    }

    /**
     * Parse non-expired lock token.
     * @param lockValue lock values
     * @return Current lock token, if any
     */
    protected static String parseValidLockToken(byte[] lockValue) {
        if (lockValue == null || lockValue.length == 0) {
            return null;
        }

        int idx1 = Bytes.indexOf(lockValue, LOCK_PREFIX_SEPARATOR_BYTE);
        int idx2 = Bytes.indexOf(lockValue, LOCK_EXPIRING_DATE_SEPARATOR_BYTE);
        String token = Bytes.toString(lockValue, idx1 + 1, idx2 - idx1 - 1);
        long expireDate;
        try {
            expireDate = Long.parseLong(Bytes.toString(lockValue, idx2 + 1));
        } catch (NumberFormatException e) {
            // Deprecated token. Assume expired token
            if (Bytes.contains(lockValue, DEPRECATED_LOCK_SEPARATOR_BYTE)) {
                return null;
            }
            throw e;
        }

        if (isExpired(expireDate)) {
            return null;
        } else {
            return token;
        }
    }

    protected static long parseExpireDate(byte[] lockValue) {
        int idx2 = Bytes.indexOf(lockValue, LOCK_EXPIRING_DATE_SEPARATOR_BYTE);
        try {
            return Long.parseLong(Bytes.toString(lockValue, idx2 + 1));
        } catch (NumberFormatException e) {
            // Deprecated token. Assume expired token
            if (Bytes.contains(lockValue, DEPRECATED_LOCK_SEPARATOR_BYTE)) {
                return -1;
            }
            throw e;
        }
    }

    /**
     * A lock is taken if there is any lockValue in the array, and
     * the token has not expired.
     *
     *
     * @param lockValue lock values
     * @return if the lock is taken
     */
    protected static boolean isLockTaken(byte[] lockValue) {
        return parseValidLockToken(lockValue) != null;
    }

    private static boolean isExpired(long expireDate) {
        return expireDate < System.currentTimeMillis();
    }

    private byte[] readLockValue(byte[] row, byte[] qualifier) throws IOException {
        byte[] lockValue;
        lockValue = HBaseManager.act(getConnection(), tableName, table -> {
            byte[] columnFamily = this.columnFamily;

            Result result = table.get(new Get(row).addColumn(columnFamily, qualifier));
            if (result.isEmpty()) {
                return null;
            } else {
                return result.getValue(columnFamily, qualifier);
            }
        });
        return lockValue;
    }

    private Connection getConnection() {
        return hbaseManager.getConnection();
    }

    public static class IllegalLockStatusException extends IllegalStateException {
        public IllegalLockStatusException(String s) {
            super(s);
        }

        public static IllegalLockStatusException inconsistentLock(byte[] row, byte[] column, long lockToken, String currentLock,
                                                                  byte[] lockValue) {
            if (StringUtils.isEmpty(currentLock)) {
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + "Lock: " + Bytes.toString(lockValue) + ".");
            } else {
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + lockToken + " != " + currentLock.hashCode() + " from " + Bytes.toString(lockValue));
            }
        }
    }
}
