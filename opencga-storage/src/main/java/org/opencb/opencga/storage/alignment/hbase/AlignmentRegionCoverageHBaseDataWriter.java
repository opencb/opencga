package org.opencb.opencga.storage.alignment.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.alignment.AlignmentRegion;
import org.opencb.commons.bioformats.alignment.io.writers.AlignmentRegionDataWriter;
import org.opencb.commons.bioformats.alignment.stats.MeanCoverage;
import org.opencb.commons.bioformats.alignment.stats.RegionCoverage;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 2/20/14
 * Time: 4:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionCoverageHBaseDataWriter implements DataWriter<AlignmentRegion> {

    private Configuration config;
    private boolean opened = false;
    private HBaseAdmin admin;
    private HTable table;
    private String tableName;
    private String sample = "s";
    private String columnFamilyName = "c";
    private List<Put> puts;


    public AlignmentRegionCoverageHBaseDataWriter(Configuration config, String tableName) {
        this.config = config;
        this.puts = new LinkedList<>();
        this.tableName = tableName;
    }

    public AlignmentRegionCoverageHBaseDataWriter(MonbaseCredentials credentials, String tableName) {
        // HBase configuration
        config = HBaseConfiguration.create();
        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));

        this.puts = new LinkedList<>();
        this.tableName = tableName;
    }

    @Override
    public boolean open() {
        try {
            admin = new HBaseAdmin(config);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        try {
            if(!admin.tableExists(tableName)){
                HTableDescriptor ht = new HTableDescriptor(tableName);
                ht.addFamily( new HColumnDescriptor(columnFamilyName));
                admin.createTable(ht);
            }
            table = new HTable(admin.getConfiguration(), tableName);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        this.opened = true;

        return true;
    }

    @Override
    public boolean close() {

        try {
            admin.close();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        this.opened = false;
        return true;
    }

    @Override
    public boolean pre() {

        return true;
    }

    @Override
    public boolean post() {

        return true;
    }

    @Override
    public boolean write(AlignmentRegion alignmentRegion) {
        System.out.println("Write " + alignmentRegion.getStart() + "Elements " + alignmentRegion.getCoverage().getAll().length);
        RegionCoverage regionCoverage = alignmentRegion.getCoverage();
        Put put;    //All the records with same rowKey

        //write Coverage per nucleotide
        for(int i = 0; i < regionCoverage.getAll().length; i++){
            if(regionCoverage.getAll()[i] != 0){
                String rowKey = alignmentRegion.getChromosome() + "_"  + String.format("%09d", (regionCoverage.getStart()+i) );

                put = new Put(Bytes.toBytes(rowKey));

                String columnName = sample;
                //Todo jcoll: Protobuffer
                String value =  "a:"    + regionCoverage.getA()[i] +
                                ",c:"   + regionCoverage.getC()[i] +
                                ",g:"   + regionCoverage.getG()[i] +
                                ",t:"   + regionCoverage.getT()[i] +
                                ",all:" + regionCoverage.getAll()[i];

                put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
                puts.add(put);
            }
        }


        //write mean coverage
        for(MeanCoverage meanCoverage : alignmentRegion.getMeanCoverage()){
            String coverageName = meanCoverage.getName();
            float[] coverage = meanCoverage.getCoverage();

            for(int i = 0; i < coverage.length; i++){
                String rowKey = alignmentRegion.getChromosome() +
                        "_" + coverageName +
                        "_" + String.format("%08d",(meanCoverage.getInitPosition() + i ));

                put = new Put(Bytes.toBytes(rowKey));

                String columnName = sample;
                //Todo jcoll: Protobuffer
                String value = "" + coverage[i];

                put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
                puts.add(put);

            }
        }

        System.out.println("Fin Write");

        try {
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        System.out.println("Table.Puts");
        return true;
    }

    @Override
    public boolean write(List<AlignmentRegion> alignmentRegions) {
        for(AlignmentRegion alignmentRegion : alignmentRegions){
            if(!write(alignmentRegion)){
                return false;
            }
        }
        return true;
    }

    public String getSample() {
        return sample;
    }

    public void setSample(String sample) {
        this.sample = sample;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }
}
