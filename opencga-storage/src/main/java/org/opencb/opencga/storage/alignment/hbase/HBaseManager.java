package org.opencb.opencga.storage.alignment.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 7:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseManager {

    private final Configuration config;
    protected boolean opened = false;
    private HBaseAdmin admin;
    private final Map<String, HTable> tableMap = new HashMap<>();


    public HBaseManager(Properties properties){
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum", "mem10.cipf.es"));
        config.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort", "2181"));
        config.set("zookeeper.znode.parent", properties.getProperty("zookeeper.znode.parent", "/hbase"));
    }
    public HBaseManager(Configuration config) {
        this.config = config;
    }


    public boolean connect() {
        try {
            admin = new HBaseAdmin(config);
        } catch (MasterNotRunningException ex) {
            Logger.getLogger(HBaseManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ZooKeeperConnectionException ex) {
            Logger.getLogger(HBaseManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(HBaseManager.class.getName()).log(Level.SEVERE, null, ex);
        }

        this.opened = true;

        return true;
    }

    public boolean disconnect() {
        if(!opened) return true;
        try {
            admin.close();
            for(HTable table : tableMap.values()){
                table.flushCommits();
                table.close();
            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseManager.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        this.opened = false;
        return true;
    }

    public HTable getTable(String tableName){
        if(!opened) return null;
        HTable table = null;
        if(tableMap.containsKey(tableName)){
            table = tableMap.get(tableName);
        } else {
            try {
                if(admin.tableExists(tableName)){
                    table = new HTable(admin.getConfiguration(), tableName);
                    tableMap.put(tableName, table);
                } else {
                    System.out.println("Table " + tableName + " no exists.");
                }
            } catch (IOException ex) {
                Logger.getLogger(HBaseManager.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }
        return table;
    }

    public HTable createTable(String tableName){
        return createTable(tableName, null);
    }
    public HTable createTable(String tableName, String columnFamilyName){
        if(!opened) return null;
            HTable table;
            try {
                if(!admin.tableExists(tableName)){
                    HTableDescriptor ht = new HTableDescriptor(tableName);
                    admin.createTable(ht);
                }
                table = new HTable(admin.getConfiguration(), tableName);
                tableMap.put(tableName, table);
                
                if (columnFamilyName != null) {
                    if(!table.getTableDescriptor().hasFamily(Bytes.toBytes(columnFamilyName))){
                        admin.disableTable(tableName);  //Disable table to add FamilyName
                        HColumnDescriptor cf = new HColumnDescriptor(columnFamilyName);
                        admin.addColumn(tableName, cf);
                        admin.enableTable(tableName);
                    }
                }
                
            } catch (IOException ex) {
                Logger.getLogger(HBaseManager.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        return table;
    }

    public boolean isOpened() {
        return opened;
    }

    public HBaseAdmin getAdmin() {
        return admin;
    }

    
}
