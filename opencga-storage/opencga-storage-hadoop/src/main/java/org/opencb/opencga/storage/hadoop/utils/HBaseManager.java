package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created on 13/11/15
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseManager extends Configured {


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
        super(configuration);
    }

    public HBaseManager() {
        super(new Configuration());
    }

    /**
     * Performs an action over a table
     *
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, byte[], HBaseTableConsumer)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perfor
     * @throws IOException
     */
    public void act(byte[] tableName, HBaseTableConsumer func) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(getConf())) {
            act(con, tableName, func);
        }
    }

    /**
     * Performs an action over a table
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @throws IOException
     */
    public void act(Connection con, byte[] tableName, HBaseTableConsumer func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            func.accept(table);
        }
    }

    /**
     * Performs an action over a table
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @throws IOException
     */
    public void act(Connection con, String tableName, HBaseTableConsumer func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            func.accept(table);
        }
    }

    /**
     * Performs an action over a table
     *
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, byte[], HBaseTableFunction)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return          Result of the function
     * @throws IOException
     */
    public <T> T act(String tableName, HBaseTableFunction<T> func) throws IOException {
        return act(Bytes.toBytes(tableName), func);
    }

    /**
     * Performs an action over a table
     *
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, byte[], HBaseTableFunction)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return          Result of the function
     * @throws IOException
     */
    public <T> T act(byte[] tableName, HBaseTableFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Connection con = ConnectionFactory.createConnection(getConf())) {
            return act(con, tableName, func);
        }
    }

    /**
     * Performs an action over a table
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return          Result of the function
     * @throws IOException
     */
    public <T> T act(Connection con, byte[] tableName, HBaseTableFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            return func.function(table);
        }
    }

    /**
     * Performs an action over a table
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return          Result of the function
     * @throws IOException
     */
    public <T> T act(Connection con, String tableName, HBaseTableFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (Table table = con.getTable(tname)) {
            return func.function(table);
        }
    }

    /**
     * Performs an action over a table
     *
     * * This method creates a new connection for the action to perform.
     * * Create a connection is heavy. Do not use for common operations.
     * * Use {@link HBaseManager#act(Connection, String, HBaseTableAdminFunction)} when possible
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return          Result of the function
     * @throws IOException
     */
    public <T> T act(String tableName, HBaseTableAdminFunction<T> func) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(getConf())) {
            return act(con, tableName, func);
        }
    }

    /**
     * Performs an action over a table
     *
     * @param tableName Table name
     * @param func      Action to perform
     * @param <T>       Return type
     * @return          Result of the function
     * @throws IOException
     */
    public <T> T act(Connection con, String tableName, HBaseTableAdminFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tableName);
        try (
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin();
        ) {
            return func.function(table,admin);
        }
    }

    /**
     * Create default HBase table layout with one column family
     *
     * @param tableName     HBase table name
     * @param columnFamily  Column Family
     * @return boolean True if a new table was created
     * @throws IOException throws {@link IOException} from creating a connection / table
     **/
    public boolean createTableIfNeeded(Connection con, String tableName, byte[] columnFamily) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        return act(con, tableName, (table, admin) -> {
            if (!admin.tableExists(tName)) {
                HTableDescriptor descr = new HTableDescriptor(tName);
                descr.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(descr);
                return true;
            }
            return false;
        });
    }


}
