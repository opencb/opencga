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
import org.opencb.opencga.storage.datamanagers.HBaseManager;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 4:48 PM
 */
public class AlignmentRegionHBaseDataWriter implements DataWriter<AlignmentRegion> {

    private HBaseManager hBaseManager;
    private HTable table;
    private String tableName;
    private String sample = "s";
    private String columnFamilyName = "c";
    private List<Put> puts;

    private AlignmentProto.AlignmentRegion.Builder alignmentRegionBuilder;

    private int index = 0;
    private String chromosome = "";



    public AlignmentRegionHBaseDataWriter(MonbaseCredentials credentials, String tableName) {
        // HBase configuration

        hBaseManager = new HBaseManager(credentials);

        this.puts = new LinkedList<>();
        this.tableName = tableName;
    }

    public AlignmentRegionHBaseDataWriter(Configuration config, String tableName) {
        hBaseManager = new HBaseManager(config);

        this.puts = new LinkedList<>();
        this.tableName = tableName;
    }

    @Override
    public boolean open() {
        hBaseManager.connect();

        return true;
    }

    @Override
    public boolean close() {

        hBaseManager.disconnect();

        return true;
    }

    @Override
    public boolean pre() {
        table = hBaseManager.createTable(tableName,columnFamilyName);

        return true;
    }

    @Override
    public boolean post() {
        flush();
        try {
            System.out.println("Puteamos la tabla. " + puts.size());
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }


        return true;
    }

    @Override
    public boolean write(AlignmentRegion alignmentRegion) {

        /**
         * 1º Find BucketLimit
         * 2º Summarize until the limit
         * 3º Write untis the limit
         */

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

    private void summaryHeader(){


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
