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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 13/11/15.
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseManager implements AutoCloseable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HBaseManager.class);
    //    public static final Set<Connection> CONNECTIONS = new ConcurrentHashSet<>();
    private static final AtomicInteger OPEN_CONNECTIONS = new AtomicInteger(0);
    private final AtomicBoolean closeConnection = new AtomicBoolean(false);
    private final AtomicReference<Connection> connection = new AtomicReference<>(null);
    private final Configuration conf;

    @FunctionalInterface
    public interface HBaseTableConsumer {
        void accept(Table table) throws IOException;
    }

    @FunctionalInterface
    public interface HBaseTableFunction<T> {
        T function(Table table) throws IOException;
    }

    @FunctionalInterface
    public interface HBaseTableAdminFunction<T> {
        T function(Table table, Admin admin) throws IOException;
    }

    public HBaseManager(Configuration configuration) {
        this(configuration, (Connection) null);
    }

    /**
     * HBaseManager connection delegate.
     * Will share the same HBase connection, and won't close it at the end.
     * @param hBaseManager Delegated hbase manager
     */
    public HBaseManager(HBaseManager hBaseManager) {
        this(hBaseManager.getConf(), hBaseManager.getConnection());
    }

    /**
     * Constructor with an optional connection. If given, this Manager won't close the connection.
     * Is responsibility of the caller to close that connection.
     * @param configuration  Correct hbase configuration
     * @param connection     Optional connection.
     */
    public HBaseManager(Configuration configuration, Connection connection) {
        this.conf = configuration;
        this.closeConnection.set(connection == null);
        this.connection.set(connection);
    }

    public Configuration getConf() {
        return conf;
    }

    public boolean getCloseConnection() {
        return closeConnection.get();
    }

    public static int getOpenConnections() {
        return OPEN_CONNECTIONS.get();
    }

    @Override
    public void close() throws IOException {
        if (this.closeConnection.get()) {
            Connection con = this.connection.getAndSet(null);
            if (null != con) {
                LOGGER.info("Close Hadoop DB connection {}", con);
                con.close();
                OPEN_CONNECTIONS.decrementAndGet();
//                CONNECTIONS.remove(con);
            }
        }
    }

    public Connection getConnection() {
        Connection con = this.connection.get();
        if (this.closeConnection.get() && null == con) {
            while (null == con) {
                try {
                    con = ConnectionFactory.createConnection(this.getConf());
                    OPEN_CONNECTIONS.incrementAndGet();
//                    CONNECTIONS.add(con);
                    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                    LOGGER.info("Opened Hadoop DB connection {} called from {}", con, stackTrace);
                } catch (IOException e) {
                    throw new IllegalStateException("Problems opening connection to DB", e);
                }
                if (!this.connection.compareAndSet(null, con)) {
                    try {
                        con.close();
                        OPEN_CONNECTIONS.decrementAndGet();
//                        CONNECTIONS.remove(con);
                    } catch (IOException e) {
                        throw new IllegalStateException("Problems closing connection to DB", e);
                    }
                }
                con = this.connection.get();
            }
        }
        return con;
    }

    /**
     * Performs an action over a table.
     *
     * @param con HBase connection object
     * @param tableName Table name
     * @param func      Action to perform
     * @throws IOException If any IO problem occurs
     */
    public static void act(Connection con, String tableName, HBaseTableConsumer func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            func.accept(table);
        }
    }

    /**
     * Performs an action over a table.
     *
     * @param con HBase connection object
     * @param tableName Table name
     * @param func      Action to perform
     * @throws IOException If any IO problem occurs
     */
    public static void act(Connection con, byte[] tableName, HBaseTableConsumer func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            func.accept(table);
        }
    }

    /**
     * Performs an action over a table.
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @throws IOException If any IO problem occurs
     */
    public void act(String tableName, HBaseTableConsumer func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = getConnection().getTable(tname)) {
            func.accept(table);
        }
    }

    /**
     * Performs an action over a table.
     * <p>
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, byte[], HBaseTableFunction)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return Result of the function
     * @throws IOException If any IO problem occurs
     */
    public <T> T act(String tableName, HBaseTableFunction<T> func) throws IOException {
        return act(Bytes.toBytes(tableName), func);
    }

    /**
     * Performs an action over a table.
     * <p>
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, byte[], HBaseTableFunction)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return Result of the function
     * @throws IOException If any IO problem occurs
     */
    public <T> T act(byte[] tableName, HBaseTableFunction<T> func) throws IOException {
        return act(getConnection(), tableName, func);
    }

    /**
     * Performs an action over a table.
     *
     * @param con HBase connection object
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return Result of the function
     * @throws IOException If any IO problem occurs
     */
    public static <T> T act(Connection con, byte[] tableName, HBaseTableFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            return func.function(table);
        }
    }

    /**
     * Performs an action over a table.
     *
     * @param con HBase connection object
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return Result of the function
     * @throws IOException If any IO problem occurs
     */
    public static <T> T act(Connection con, String tableName, HBaseTableFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            return func.function(table);
        }
    }

    /**
     * Performs an action over a table.
     * <p>
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, String, HBaseTableAdminFunction)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return Result of the function
     * @throws IOException If any IO problem occurs
     */
    public <T> T act(String tableName, HBaseTableAdminFunction<T> func) throws IOException {
        return act(getConnection(), tableName, func);
    }

    /**
     * Performs an action over a table.
     *
     * @param con HBase connection object
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return Result of the function
     * @throws IOException If any IO problem occurs
     */
    public static <T> T act(Connection con, String tableName, HBaseTableAdminFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname); Admin admin = con.getAdmin()) {
            return func.function(table, admin);
        }
    }

    public static boolean createNamespaceIfNeeded(Connection con, String namespace) throws IOException {
        try (Admin admin = con.getAdmin()) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            if (!namespaceExists(namespace, admin)) {
                try {
                    admin.createNamespace(namespaceDescriptor);
                } catch (NamespaceExistException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            return true;
        }
    }

    static boolean namespaceExists(String namespace, Admin admin) throws IOException {
        try {
            admin.getNamespaceDescriptor(namespace);
            return true;
        } catch (NamespaceNotFoundException ignored) {
            return false;
        }

    }

    /**
     * Checks if the required table exists.
     *
     * @param tableName    HBase table name
     * @return boolean True if the table exists
     * @throws IOException throws {@link IOException}
     **/
    public boolean tableExists(String tableName) throws IOException {
        return act(tableName, (table, admin) -> admin.tableExists(table.getName()));
    }

    /**
     * Create default HBase table layout with one column family.
     *
     * @param tableName    HBase table name
     * @param columnFamily Column Family
     * @param compressionType Compression Algorithm
     * @return boolean True if a new table was created
     * @throws IOException throws {@link IOException} from creating a connection / table
     **/
    public boolean createTableIfNeeded(String tableName, byte[] columnFamily, Compression.Algorithm compressionType)
            throws IOException {
        return createTableIfNeeded(getConnection(), tableName, columnFamily, Collections.emptyList(), compressionType);
    }

    /**
     * Create default HBase table layout with one column family.
     *
     * @param tableName    HBase table name
     * @param columnFamily Column Family
     * @param preSplits Pre-split regions at table creation
     * @param compressionType Compression Algorithm
     * @return boolean True if a new table was created
     * @throws IOException throws {@link IOException} from creating a connection / table
     **/
    public boolean createTableIfNeeded(String tableName, byte[] columnFamily,
                                              List<byte[]> preSplits, Compression.Algorithm compressionType)
            throws IOException {
        return createTableIfNeeded(getConnection(), tableName, columnFamily, preSplits, compressionType);
    }

    /**
     * Create default HBase table layout with one column family.
     *
     * @param con HBase connection object
     * @param tableName    HBase table name
     * @param columnFamily Column Family
     * @param preSplits Pre-split regions at table creation
     * @param compressionType Compression Algorithm
     * @return boolean True if a new table was created
     * @throws IOException throws {@link IOException} from creating a connection / table
     **/
    public static boolean createTableIfNeeded(Connection con, String tableName, byte[] columnFamily,
                                              List<byte[]> preSplits, Compression.Algorithm compressionType)
            throws IOException {
        TableName tName = TableName.valueOf(tableName);
        LOGGER.debug("Create table if needed with connection {}", con);
        return act(con, tableName, (table, admin) -> {
            if (!admin.tableExists(tName)) {
                HTableDescriptor descr = new HTableDescriptor(tName);
                HColumnDescriptor family = new HColumnDescriptor(columnFamily);
                if (compressionType != null) {
                    family.setCompressionType(compressionType);
                }
                descr.addFamily(family);
                try {
                    if (preSplits != null && !preSplits.isEmpty()) {
                        admin.createTable(descr, preSplits.toArray(new byte[0][]));
                        LOGGER.info("Create New HBASE table {} with {} preSplits", tableName, preSplits.size());
                    } else {
                        admin.createTable(descr);
                        LOGGER.info("Create New HBASE table - no pre-splits {}", tableName);
                    }
                } catch (TableExistsException e) {
                    return false;
                }
                return true;
            }
            return false;
        });
    }

    public static Configuration addHBaseSettings(Configuration conf, String credentialsStr) throws URISyntaxException {
        HBaseCredentials credentials = new HBaseCredentials(credentialsStr);
        return addHBaseSettings(conf, credentials);
    }

    public static Configuration addHBaseSettings(Configuration conf, HBaseCredentials credentials) {
        // Do not overwrite the input conf
        HBaseConfiguration.addHbaseResources(conf);

        if (StringUtils.isNotEmpty(credentials.getZookeeperQuorums())) {
            conf.set(HConstants.ZOOKEEPER_QUORUM, credentials.getZookeeperQuorums());
        }
        //ZKConfig.getZKQuorumServersString(conf)

//        conf.set("hbase.master", credentials.getHostAndPort());

        // Skip default values
        if (!credentials.isDefaultZookeeperClientPort()) {
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(credentials.getHbaseZookeeperClientPort()));
        }
        if (!credentials.isDefaultZookeeperZnode()) {
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, credentials.getZookeeperZnode());
        }
        return conf;
    }
}
