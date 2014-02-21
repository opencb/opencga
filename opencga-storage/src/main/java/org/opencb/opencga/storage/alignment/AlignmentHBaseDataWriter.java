package org.opencb.opencga.storage.alignment;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.alignment.io.writers.AlignmentDataWriter;

import java.io.IOException;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 12/3/13
 * Time: 6:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentHBaseDataWriter implements AlignmentDataWriter<SAMRecord,SAMFileHeader> {

    private Configuration config;
    private boolean opened = false;
    private HBaseAdmin admin;
    private HTable table;
    private String tableName;
    private String sample = "s";
    private String columnFamilyName = "c";
    private int chunkSize;
    private String chunkSizeName;
    private Put put;    //All the records with same rowKey
    private int chunkStart; //Min value for the AlignmentStart in the same rowKey
    private int chunkEnd; //Max value
    private int recordNum;
    private String referenceName; //Name of the last reference name



    public AlignmentHBaseDataWriter(Configuration config, String tableName) {
        this.config = config;
        this.tableName = tableName;
        this.setChunkSize(1000, "1K");
    }

    public boolean setChunkSize(int chunkSize, String chunkSizeName) {
        if(!opened){
            this.chunkSize = chunkSize;
            this.chunkSizeName = chunkSizeName;
            return true;
        } else {
            return false;
        }
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
                admin.createTable( ht );
            }
            table = new HTable(admin.getConfiguration(), tableName);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        this.opened = true;
        this.chunkStart = 0;
        this.chunkEnd = -1;
        this.recordNum = 0;
        this.referenceName = "1";

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
    public boolean write(SAMRecord element) {
        Integer start = element.getAlignmentStart();
        String referenceName = element.getReferenceName();

        if (!(chunkEnd > start && chunkStart <= start) || referenceName != this.referenceName) {
            try {
                if (put != null) {
                    table.put(put);
                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return false;
            }
            Integer numChunk = (start/chunkSize);
            String numChunkName = String.format("_%06d_",numChunk);
            String rowKey = referenceName  + numChunkName  + chunkSizeName;
            this.referenceName = referenceName;

            chunkStart = numChunk* chunkSize;
            chunkEnd = chunkStart + chunkSize;
            recordNum = 0;

            put = new Put(Bytes.toBytes(rowKey));
        }
        String columnName = sample;
        //String record = element.getReadString();
        //String record = start.toString();
        String record = "st:" + start.toString() + ",end:" + element.getAlignmentEnd() + ",r:"+element.getReadString();
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName+String.format("_%04d",recordNum)), Bytes.toBytes(record));
        recordNum++;

    /*
        PrintStream writer = System.out;
        writer.println(element.getReadName());
        writer.println(element.getFlags());
        writer.println(element.getReferenceName());
        writer.println(element.getAlignmentStart());
        writer.println(element.getMappingQuality());
        writer.println(element.getCigarString());
        writer.println(element.getMateReferenceName());
        writer.println(element.getMateAlignmentStart());
       // writer.println(element.getAlignmentBlocks() );

        writer.println(element.getReadString());
        writer.println(element.getBaseQualityString());


        writer.println(element.getSAMString());


        writer.println(element.toString());*/
        return true;
    }

    @Override
    public boolean write(List<SAMRecord> samRecords) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public boolean writeHeader(SAMFileHeader head) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
