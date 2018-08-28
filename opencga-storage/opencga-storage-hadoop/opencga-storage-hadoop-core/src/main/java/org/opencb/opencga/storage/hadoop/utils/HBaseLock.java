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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
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
 *           return TRUE;
 *      } else {
 *           // Token already taken
 *           return FALSE;
 *      }
 *
 * Unlock:
 *      HBase.put(row, column, "");
 *
 * Created on 19/05/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseLock {

    private static final String LOCK_SEPARATOR = "_";
    private static final String LOCK_EXPIRING_DATE_SEPARATOR = ":";
    private static final String CURRENT_LOCK = "CURRENT-";

    protected final HBaseManager hbaseManager;
    protected final String tableName;
    protected final byte[] columnFamily;
    protected final byte[] defaultRow;
    protected static Logger logger = LoggerFactory.getLogger(HBaseLock.class);

    public HBaseLock(HBaseManager hbaseManager, String tableName, byte[] columnFamily, byte[] row) {
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
    public long lock(byte[] column, long lockDuration, long timeout)
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
    public long lock(byte[] row, byte[] column, long lockDuration, long timeout)
            throws InterruptedException, TimeoutException, IOException {
        String token = RandomStringUtils.randomAlphanumeric(10);

        // Minimum lock duration of 100ms
        lockDuration = Math.max(lockDuration, 100);

        String[] lockValue;
        String readToken = "";
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        lockValue = readLockValue(row, column);
        do {
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

            // Append token to the lock cell
            appendToken(token, lockDuration, row, column);

            lockValue = readLockValue(row, column);

            // Get the first non expired lock
            for (String lock : lockValue) {
                if (!isLockExpired(lock)) {
                    readToken = lock.split(LOCK_EXPIRING_DATE_SEPARATOR)[0];
                    break;
                }
            }

            // You win the lock if the first available lock is yours.
        } while (!readToken.equals(token));

        logger.debug("Won the lock with token " + token + " (" + token.hashCode() + ") from lock: " + Arrays.toString(lockValue));
        // Overwrite the lock with the winner current lock. Remove previous expired locks
        putCurrentLock(token, lockDuration, row, column);

        return token.hashCode();
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
        String[] lockValue;
        lockValue = readLockValue(row, column);

        String currentLock = "";
        for (String lock : lockValue) {
            if (lock.startsWith(CURRENT_LOCK)) {
                currentLock = lock.replace(CURRENT_LOCK, "").split(LOCK_EXPIRING_DATE_SEPARATOR)[0];
                break;
            }
        }

        if (currentLock.hashCode() != lockToken) {
            throw new IllegalLockStatusException(row, column, lockToken, currentLock, lockValue);
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
                            CURRENT_LOCK
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
     * A lock is taken if there is any lockValue in the array, and
     * the token has not expired.
     *
     *
     * @param lockValue
     * @return
     */
    private boolean isLockTaken(String[] lockValue) {
        for (String lock : lockValue) {
            if (lock.startsWith(CURRENT_LOCK)) {
                return !isLockExpired(lock);
            }
        }
        return false;
    }

    private boolean isLockExpired(String lock) {
        String[] split = lock.split(LOCK_EXPIRING_DATE_SEPARATOR);
        long expireDate = Long.parseLong(split[1]);
        return expireDate < System.currentTimeMillis();
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
        IllegalLockStatusException(byte[] row, byte[] column, long lockToken, String currentLock, String[] lockValue) {
            super("Inconsistent lock status. You don't have the lock! "
                    + "Row: '" + Bytes.toStringBinary(row) + "', "
                    + "column: '" + Bytes.toStringBinary(column) + "'. "
                    + lockToken + " != " + currentLock.hashCode() + " from " + Arrays.toString(lockValue));
        }
    }
}
