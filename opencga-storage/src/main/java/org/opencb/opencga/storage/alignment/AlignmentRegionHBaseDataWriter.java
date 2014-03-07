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

    long index = 0;



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
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean write(AlignmentRegion alignmentRegion) {
        Put put = null;    //All the records with same rowKey
        String rowKey;
        String value;


        if(index == 0){
            index = alignmentRegion.getStart() >> 8;// start / 256
            alignmentRegionBuilder = AlignmentProto.AlignmentRegion.newBuilder();
        }


        for(Alignment alignment : alignmentRegion.getAlignments()){

            if(index < (alignment.getStart() >> 8)){     //create new put() and new rowKey
                rowKey = alignment.getChromosome() + "_" + String.format("%07d", index);
                System.out.println("Creamos un Put() con rowKey " + rowKey);

                put = new Put(Bytes.toBytes(rowKey));
                if(alignmentRegionBuilder != null){
                    put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), alignmentRegionBuilder.build().toByteArray());
                }
                puts.add(put);

                index = alignment.getStart() >> 8;
                alignmentRegionBuilder = AlignmentProto.AlignmentRegion.newBuilder();
            }



            AlignmentProto.AlignmentRecord.Builder alignmentRecordBuilder = AlignmentProto.AlignmentRecord.newBuilder()
                    .setName(alignment.getName())
                    .setFlags(alignment.getFlags())
                    .setIncrementalPos((int)alignment.getStart())   //TODO jj: Real Incremental Pos
                    .setMapq(alignment.getMappingQuality())
                    .setRnext("rnext")
                    .setRelativePnext(0)
                    .setLen(alignment.getLength());

            for(Alignment.AlignmentDifference alignmentDifference : alignment.getDifferences()){
                AlignmentProto.Difference.DifferenceOperator operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH;
                switch(alignmentDifference.getOp()){
                    case Alignment.AlignmentDifference.DELETION:
                        operator = AlignmentProto.Difference.DifferenceOperator.DELETION;
                        break;
                    case Alignment.AlignmentDifference.HARD_CLIPPING:
                        operator = AlignmentProto.Difference.DifferenceOperator.HARD_CLIPPING;
                        break;
                    case Alignment.AlignmentDifference.INSERTION:
                        operator = AlignmentProto.Difference.DifferenceOperator.INSERTION;
                        break;
                    case Alignment.AlignmentDifference.MISMATCH:
                        operator = AlignmentProto.Difference.DifferenceOperator.MISMATCH;
                        break;
                    case Alignment.AlignmentDifference.PADDING:
                        operator = AlignmentProto.Difference.DifferenceOperator.PADDING;
                        break;
                    case Alignment.AlignmentDifference.SKIPPED_REGION:
                        operator = AlignmentProto.Difference.DifferenceOperator.SKIPPED_REGION;
                        break;
                    case Alignment.AlignmentDifference.SOFT_CLIPPING:
                        operator = AlignmentProto.Difference.DifferenceOperator.SOFT_CLIPPING;
                        break;

                }
                alignmentRecordBuilder.addDiffs(AlignmentProto.Difference.newBuilder()
                        .setOperator(operator)
                        .setRelativePos(alignmentDifference.getPos())
                        .setLength(alignmentDifference.getLength())
                        .build()
                );
            }

            alignmentRegionBuilder.addAlignmentRecords(alignmentRecordBuilder.build());

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
