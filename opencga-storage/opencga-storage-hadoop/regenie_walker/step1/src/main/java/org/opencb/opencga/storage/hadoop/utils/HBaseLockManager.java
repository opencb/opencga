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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
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
    // Make non-final for testing purposes
    protected static ExecutorService threadPool = buildThreadPool();

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

        LockToken lockToken;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        do {
            lockToken = readLockToken(row, column);

            // If the lock is taken, wait
            while (lockToken.isTaken()) {
                Thread.sleep(100);
                lockToken = readLockToken(row, column);
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
            if (tryToPutToken(token, lockDuration, row, column, lockToken, CURRENT)) {
                lockToken = readLockToken(row, column);
            }

            // You win the lock if you manage to write your lock.
        } while (!lockToken.equals(token));

        boolean prevTokenExpired = !lockToken.isEmpty() && lockToken.isExpired();
        boolean slowQuery = stopWatch.getTime() > 60000;
        if (prevTokenExpired || slowQuery) {
            StringBuilder msg = new StringBuilder("Lock column '").append(Bytes.toStringBinary(column)).append("'");
            if (prevTokenExpired) {
                long expireDate = lockToken.getExpireDate();
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

        logger.debug("Won the lock with token " + token + " (" + token.hashCode() + ")");

        return new HBaseLock(lockDuration, token, row, column);
    }

    /**
     * Refreshes the lock.
     *
     * @param row           Row to find the lock cell
     * @param column        Column to find the lock cell
     * @param lockTokenHash Lock token
     * @param lockDuration  Duration un milliseconds of the token. After this time the token is expired.
     * @throws IOException      if there is an error writing or reading from HBase.
     */
    private void refresh(byte[] row, byte[] column, long lockTokenHash, long lockDuration) throws IOException {
        // Check token is valid
        LockToken currentLockToken = readLockToken(row, column);
        if (currentLockToken.isEmpty() || currentLockToken.isExpired() || !currentLockToken.equals(lockTokenHash)) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockTokenHash, currentLockToken);
        }
        if (currentLockToken.getRemainingTime() < lockDuration / 2) {
            logger.warn("Refreshing lock with less than half of the duration remaining. Expected duration: {} Remaining time: {}ms",
                    lockDuration,
                    currentLockToken.getRemainingTime());
        }

        if (!tryToPutToken(currentLockToken.token, lockDuration, row, column, currentLockToken, REFRESH)) {
            // Error refreshing!
            LockToken newLockToken = readLockToken(row, column);

            logger.error("Current lock token:" + currentLockToken.token);
            logger.error("New lock token: " + newLockToken.token);
            throw IllegalLockStatusException.inconsistentLock(row, column, lockTokenHash, currentLockToken);
        }
    }

    /**
     * Releases the lock.
     *
     * @param row       Row to find the lock cell
     * @param column    Column to find the lock cell
     * @param lockTokenHash Lock token
     * @throws IOException                if there is an error writing or reading from HBase.
     * @throws IllegalLockStatusException if the lockToken does not match with the current lockToken
     */
    private void unlock(byte[] row, byte[] column, long lockTokenHash) throws IOException, IllegalLockStatusException {
        LockToken currentToken = readLockToken(row, column);

        if (currentToken.isEmpty() || currentToken.isExpired() || !currentToken.equals(lockTokenHash)) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockTokenHash, currentToken);
        }

        logger.debug("Unlock lock with token " + lockTokenHash);
        if (!clearLock(row, column, currentToken)) {
            throw IllegalLockStatusException.inconsistentLock(row, column, lockTokenHash, currentToken);
        }
    }

    private Boolean tryToPutToken(String token, long lockDuration, byte[] row, byte[] qualifier, LockToken currentLock, String type)
            throws IOException {
        return hbaseManager.act(tableName, table -> {
            Put put = new Put(row)
                    .addColumn(columnFamily, qualifier, Bytes.toBytes(
                            type
                                    + token
                                    + LOCK_EXPIRING_DATE_SEPARATOR_STR
                                    + (System.currentTimeMillis() + lockDuration)));
            return table.checkAndPut(row, columnFamily, qualifier, CompareFilter.CompareOp.EQUAL, currentLock.lockValue, put);
        });
    }

    private boolean clearLock(byte[] row, byte[] qualifier, LockToken lockToken) throws IOException {
        return hbaseManager.act(tableName, table -> {
            Put put = new Put(row)
                    .addColumn(columnFamily, qualifier, Bytes.toBytes(""));
            return table.checkAndPut(row, columnFamily, qualifier, CompareFilter.CompareOp.EQUAL, lockToken.lockValue, put);
        });
    }

    /**
     * Parse lock token.
     * @param lockValue lock values
     * @return Current lock token.
     */
    protected static LockToken parseLockToken(byte[] lockValue) {
        if (lockValue == null || lockValue.length == 0) {
            return new LockToken();
        }

        int idx1 = Bytes.indexOf(lockValue, LOCK_PREFIX_SEPARATOR_BYTE);
        int idx2 = Bytes.indexOf(lockValue, LOCK_EXPIRING_DATE_SEPARATOR_BYTE);
        String type = Bytes.toString(lockValue, 0, idx1);
        String token = Bytes.toString(lockValue, idx1 + 1, idx2 - idx1 - 1);
        long expireDate;
        try {
            expireDate = Long.parseLong(Bytes.toString(lockValue, idx2 + 1));
        } catch (NumberFormatException e) {
            // Deprecated token. Assume expired token
            if (Bytes.contains(lockValue, DEPRECATED_LOCK_SEPARATOR_BYTE)) {
                return new LockToken();
            }
            throw e;
        }
        return new LockToken(lockValue, type, token, expireDate);
    }

    protected static final class LockToken {
        protected final byte[] lockValue;
        protected final String type;
        protected final String token;
        protected final Long expireDate;

        private LockToken() {
            this.lockValue = new byte[0];
            this.type = null;
            this.token = null;
            this.expireDate = null;
        }

        private LockToken(byte[] lockValue, String type, String token, long expireDate) {
            this.lockValue = lockValue;
            this.type = type;
            this.token = token;
            this.expireDate = expireDate;
        }

        /**
         * A lock is taken if there is any lockValue, and
         * the token has not expired.
         *
         * @return if the lock is taken
         */
        public boolean isTaken() {
            return token != null && !isExpired();
        }

        public boolean isExpired() {
            return expireDate != null && expireDate < System.currentTimeMillis();
        }

        public boolean isEmpty() {
            return token == null;
        }

        public boolean equals(String token) {
            return !isEmpty() && this.token.equals(token);
        }

        public boolean equals(long tokenHash) {
            return !isEmpty() && this.token.hashCode() == tokenHash;
        }

        public byte[] getLockValue() {
            return lockValue;
        }

        public String getType() {
            return type;
        }

        public String getToken() {
            return token;
        }

        public Long getExpireDate() {
            return expireDate;
        }

        public long getRemainingTime() {
            return expireDate == null ? 0 : expireDate - System.currentTimeMillis();
        }
    }

    private LockToken readLockToken(byte[] row, byte[] qualifier) throws IOException {
        return parseLockToken(readLockValue(row, qualifier));
    }

    private byte[] readLockValue(byte[] row, byte[] qualifier) throws IOException {
        byte[] lockValue;
        lockValue = hbaseManager.act(tableName, table -> {
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

    public static class IllegalLockStatusException extends IllegalStateException {
        public IllegalLockStatusException(String s) {
            super(s);
        }

        private static IllegalLockStatusException inconsistentLock(byte[] row, byte[] column, long lockTokenHash, LockToken currentLock) {
            if (currentLock.isEmpty()) {
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! Empty lock. "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + "Lock: " + Bytes.toString(currentLock.lockValue) + ".");
            } else if (currentLock.isExpired()) {
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! Expired lock. "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + "Lock: " + Bytes.toString(currentLock.lockValue) + ".");
            } else {
                return new IllegalLockStatusException("Inconsistent lock status. You don't have the lock! Lock is taken. "
                        + "Row: '" + Bytes.toStringBinary(row) + "', "
                        + "column: '" + Bytes.toStringBinary(column) + "'. "
                        + lockTokenHash + " != " + currentLock.token.hashCode() + " from " + Bytes.toString(currentLock.lockValue));
            }
        }
    }

    protected static ExecutorService buildThreadPool() {
        return Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                        .setNameFormat("hbase-lock-%d")
                        .build());
    }

    private final class HBaseLock extends Lock {
        private final long lockDuration;
        private final String token;
        private final long tokenHash;
        private final byte[] row;
        private final byte[] column;

        private HBaseLock(long lockDuration, String token, byte[] row, byte[] column) {
            super(HBaseLockManager.threadPool, (int) (lockDuration / 4), token.hashCode());
            this.lockDuration = lockDuration;
            this.token = token;
            this.tokenHash = token.hashCode();
            this.row = row;
            this.column = column;
        }

        @Override
        public void unlock0() {
            try {
                synchronized (this) {
                    HBaseLockManager.this.unlock(row, column, tokenHash);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void refresh() throws IOException {
            synchronized (this) {
                HBaseLockManager.this.refresh(row, column, tokenHash, lockDuration);
            }
        }
    }
}
