package org.opencb.opencga.storage.alignment;

import org.apache.hadoop.conf.Configuration;
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
import java.util.ArrayList;
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


    private int alignmentBucketSize = 256;

    //

    List<Alignment> alignmentsRemain = new LinkedList<>();
    private int summaryIndex = 0;
    private long index = 0;
    private String chromosome = "";

    // alignments overlapped along several buckets
    private ArrayList<Integer> numBucketsOverlapped = new ArrayList<>(100);
    private long lastOverlappedPosition = 0;
    private long currentBucket = 0;



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

        /*
         * 1º Add remaining alignments from last AR
         * 2º Take Alignments from tail
         * 3º Create summary
         * 4º Create Proto
         * 5º Write into hbase
         *
         */

        String value;
        Alignment firstAlignment = alignmentRegion.getAlignments().get(0);

        if(firstAlignment == null){
            return false;
        }

        //First alignment. Init and writes headers
        if(index == 0){
            index = firstAlignment.getStart() / alignmentBucketSize;
            chromosome = firstAlignment.getChromosome();

            globalHeader();
            chromosomeHeader();
        }
        //Changes chromosome. init and write chromosomeHeader.
        if(!chromosome.equals(firstAlignment.getChromosome())){
            //TODO: CUIDAOOOO!!! SI cambiamos de cromosoma, que hacemos con los ultimos??
            //TODO: A LIARLA! THIS IS PARCHEEE!
            alignmentsRemain.clear();
            //FIXME! jj! MISSING ALIGNMENTS!
            //TODO FIXME!!
            //FIXME PLEASE!!
            index = firstAlignment.getStart() / alignmentBucketSize;
            chromosome = firstAlignment.getChromosome();
            summaryIndex = 0;       //Set to 0, only if the summary rowkey has the chromosome.
            chromosomeHeader();
        }
        firstAlignment = null;  //Don't needed anymore.


        //1º Add remaining alignments.
        List<Alignment> alignments = alignmentsRemain;
        alignments.addAll(alignmentRegion.getAlignments());

        //2º Take alignments from tail.
        alignmentsRemain = new LinkedList<>();
        Alignment alignmentAux = alignments.remove(alignments.size()-1);    //Remove last
        alignmentsRemain.add(0, alignmentAux);
        long lastBucket = alignmentAux.getStart()/alignmentBucketSize;

        while(alignments.size() != 0){
            if(alignments.get(alignments.size()).getStart()/alignmentBucketSize != lastBucket){
                break;
            } else {
                alignmentsRemain.add(0, alignments.remove(alignments.size() - 1));    //Remove last
            }
        }

        //3º Create Summary
        currentBucket = alignments.get(0).getStart()/alignmentBucketSize;
        lastOverlappedPosition = alignments.get(0).getUnclippedEnd();

        AlignmentRegionSummary summary = new AlignmentRegionSummary(summaryIndex);
        for(Alignment alignment : alignments){
            summary.addAlignment(alignment);
            setOverlaps(alignment);
        }
        summary.close();
        putSummary(summary);
        summaryIndex++;

        //4º Create Proto
        List<Alignment> alignmentBucketList = new LinkedList<>();
        for(Alignment alignment : alignments){
            if(index < alignment.getStart()/alignmentBucketSize){
                putBucket(AlignmentProtoHelper.toAlignmentBucketProto(alignmentBucketList, summary, index*alignmentBucketSize), numBucketsOverlapped.get((int)index));
                index = alignment.getStart() / alignmentBucketSize;
                chromosome = alignment.getChromosome();
            }
            alignmentBucketList.add(alignment);
        }


        //5º Write into hbase
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

    private void chromosomeHeader() { //TODO jj:
        //To change body of created methods use File | Settings | File Templates.
    }


    private void putSummary(AlignmentRegionSummary summary){
        String rowKey = "S_" + chromosome + "_" + index;

        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] compress;
        try {
            compress = Snappy.compress(summary.toProto().toByteArray());
        } catch (IOException e) {
            System.out.println("this AlignmentProto.Summary could not be compressed by snappy");
            e.printStackTrace();  // TODO jj handle properly
            return;
        }
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), compress);

        puts.add(put);
    }

    private void putBucket(AlignmentProto.AlignmentBucket alignmentBucket){
        if(alignmentBucket == null)
            return;

        String rowKey = chromosome + "_" + String.format("%07d", index);
        //System.out.println("Creamos un Put() con rowKey " + rowKey);

        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] compress;
        try {
            compress = Snappy.compress(alignmentBucket.toByteArray());
        } catch (IOException e) {
            System.out.println("this AlignmentProto.AlignmentBucket could not be compressed by snappy");
            e.printStackTrace();  // TODO jj handle properly
            return;
        }
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), compress);

        puts.add(put);
    }

    /**
     * Updates the array of overlaps.
     * An overlap of 2 in bucket 7 means that some alignment in
     * bucket 7-2=5 is long enough to end in bucket 7.
     */
    private void setOverlaps(Alignment alignment) { // TODO jj: test
        if (alignment.getStart() / alignmentBucketSize > currentBucket) {   // finished this bucket
            for (int i = 0; i <= lastOverlappedPosition/alignmentBucketSize - currentBucket; i++) { // write bucket overlaps
                int previousOverlap = numBucketsOverlapped.get((int) currentBucket + i);    // get overlap already stored
                if (previousOverlap < i) {
                    numBucketsOverlapped.set((int) currentBucket + i, i);
                }
            }
            currentBucket = alignment.getStart() / alignmentBucketSize;
            lastOverlappedPosition = alignment.getUnclippedEnd();
        }
        lastOverlappedPosition = ((lastOverlappedPosition > alignment.getUnclippedEnd()) ? lastOverlappedPosition : alignment.getUnclippedEnd()); // max
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
