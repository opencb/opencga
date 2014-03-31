package org.opencb.opencga.storage.alignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.bioformats.alignment.AlignmentRegion;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 4:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionHBaseDataWriter implements DataWriter<AlignmentRegion> {

    //Too similar to AlignmentRegionCoverageHBaseDataWriter. TODO jj: Common HBase writer class.
    private Configuration config;
    private boolean opened = false;
    private HBaseAdmin admin;
    private HTable table;
    private String tableName;
    private String sample = "s";
    private String columnFamilyName = "c";
    private List<Put> puts;

    AlignmentProto.AlignmentRegion.Builder alignmentRegionBuilder;

    int index = 0;
    String chromosome = "";



    public AlignmentRegionHBaseDataWriter(MonbaseCredentials credentials, String tableName) {
        // HBase configuration
        config = HBaseConfiguration.create();
        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));

        this.puts = new LinkedList<>();
        this.tableName = tableName;
    }

    public AlignmentRegionHBaseDataWriter(Configuration config, String tableName) {
        this.config = config;
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
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean post() {
        flush();
        try {
            System.out.println("Puteamos la tabla. " + puts.size());
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        return true;
    }

    @Override
    public boolean write(AlignmentRegion alignmentRegion) {

        String value;
        Alignment firstAlignment = alignmentRegion.getAlignments().get(0);

        if(firstAlignment == null){
            return false;
        }

        if(index == 0){
            init(firstAlignment);

           // globalHeader();
           // chromosomeHeader();
        }
        System.out.println("Chromosome: " + alignmentRegion.getChromosome()
                + " size: " + alignmentRegion.getAlignments().size()
                + " last: " + alignmentRegion.getAlignments().get(alignmentRegion.getAlignments().size()-1).getStart());
        if(!chromosome.equals(firstAlignment.getChromosome())){
            flush();
            chromosomeHeader();
            init(firstAlignment);
        }


        for(Alignment alignment : alignmentRegion.getAlignments()){
            if(index < (alignment.getStart() >> 8)){    //flush and reset
               flush();
               init(alignment);
            }

            alignmentRegionBuilder.addAlignmentRecords(AlignmentProtoHelper.toProto(alignment, index << 8));

        }


        try {
            System.out.println("Puteamos la tabla. " + puts.size());
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        return true;

    }

    private void globalHeader() { //TODO jj:
        //To change body of created methods use File | Settings | File Templates.
    }

    private void chromosomeHeader() { //Write Chromosome header AFTER write and flush all the chromosome
        //ROW_KEY = header_<chromosome>
        String rowKey = "header_" + chromosome;
        System.out.println("Header : " + rowKey);

        Put put = new Put(Bytes.toBytes(rowKey));
        AlignmentProto.Header.Region.Builder headerBuilder = AlignmentProto.Header.Region.newBuilder()
                .setChromosomeName(chromosome)
                .setMaxValue(index);
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), headerBuilder.build().toByteArray());

        puts.add(put);

    }


    private void init(Alignment alignment){
        index = (int)(alignment.getStart() >> 8);
        chromosome = alignment.getChromosome();
        alignmentRegionBuilder = AlignmentProto.AlignmentRegion.newBuilder();
    }



    private void flush(){
        String rowKey = chromosome + "_" + String.format("%07d", index);
        //System.out.println("Creamos un Put() con rowKey " + rowKey);

        Put put = new Put(Bytes.toBytes(rowKey));
        if(alignmentRegionBuilder != null){
            byte[] compress;
            try {
                compress = Snappy.compress(alignmentRegionBuilder.build().toByteArray());
            } catch (IOException e) {
                System.out.println("this AlignmentProto.AlignmentRegion could not be compressed by snappy");
                e.printStackTrace();  // TODO jj handle properly
                return;
            }
            put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), compress);
        }
        puts.add(put);
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
