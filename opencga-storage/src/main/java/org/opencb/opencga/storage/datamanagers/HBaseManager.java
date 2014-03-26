package org.opencb.opencga.storage.datamanagers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 7:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseManager {

    private Configuration config;
    protected boolean opened = false;
    private HBaseAdmin admin;
    private List<HTable> tableList = new LinkedList<>();


    public HBaseManager(Configuration config) {
        this.config = config;
    }

    public HBaseManager(MonbaseCredentials credentials) {
        // HBase configuration
        config = HBaseConfiguration.create();
        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
    }

    public boolean connect() {
        try {
            admin = new HBaseAdmin(config);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        this.opened = true;

        return true;
    }

    public boolean disconnect() {
        if(!opened) return true;
        try {
            admin.close();
            for(HTable table : tableList){
                table.flushCommits();
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        this.opened = false;
        return true;
    }

    public HTable getTable(String tableName){
        if(!opened) return null;
        HTable table = null;
        try {
            if(!admin.tableExists(tableName)){
                table = new HTable(admin.getConfiguration(), tableName);
                tableList.add(table);
            }
        } catch (IOException e) {
            e.printStackTrace();
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
                if(columnFamilyName != null)
                    ht.addFamily( new HColumnDescriptor(columnFamilyName));
                admin.createTable(ht);
            }
            table = new HTable(admin.getConfiguration(), tableName);
            tableList.add(table);
        } catch (IOException e) {
            e.printStackTrace();
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

    public List<HTable> getTableList() {
        return tableList;
    }

}
