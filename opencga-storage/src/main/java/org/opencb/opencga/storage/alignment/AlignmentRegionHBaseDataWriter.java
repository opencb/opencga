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
    private String sample = "generic_sample";
    private String columnFamilyName = "c";
    private List<Put> puts;


    private int alignmentBucketSize = 256;

    //
    List<Alignment> alignmentsRemain = new LinkedList<>();
    private int summaryIndex = 0;
    private String chromosome = "";

    // alignments overlapped along several buckets
    private LinkedList<Integer> numBucketsOverlapped = new LinkedList<>();
    private long bucketsOverlappedStart = 0;    // number of first bucket in numBucketsOverlapped. previous buckets are already written in summary,



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
         * 3º Split into AlignmentBucket
         *
         * 4º Create summary
         * 5º Create AlignmentProto.AlignmentBucket
         * 6º Write into hbase
         *
         */






        //if(currentBucket != bucketsOverlappedStart)


        //Changes chromosome. init and write chromosomeHeader.
        //First alignment. Init and writes headers
        if(!chromosome.equals(alignmentRegion.getChromosome())){
            //There are remaining alignments
            if(!alignmentsRemain.isEmpty()){
                long remainBucket = alignmentsRemain.get(0).getStart() / alignmentBucketSize;
                List<Alignment>[] list = new List[1];
                list[0] = alignmentsRemain;
                AlignmentRegionSummary summary = createSummary(list);
                putSummary(summary);

                int overlapped = numBucketsOverlapped.remove((int) (remainBucket - bucketsOverlappedStart));
                putBucket(AlignmentProtoHelper.toAlignmentBucketProto(alignmentsRemain, summary, remainBucket * alignmentBucketSize, overlapped), remainBucket);
            }
            alignmentsRemain.clear();
            chromosome = alignmentRegion.getChromosome();
            summaryIndex = 0;       //Set to 0, only if the summary rowkey has the chromosome.
        }


        if(alignmentRegion.getAlignments().get(0) == null){
            return false;
        }


        long currentBucket;
        if(alignmentsRemain.isEmpty()){ //Only empty when starts new chromosome.
            currentBucket = alignmentRegion.getAlignments().get(0).getStart() / alignmentBucketSize;
            numBucketsOverlapped.clear();
            numBucketsOverlapped.add(0);    //First bucket has 0 overlapped;
            bucketsOverlappedStart = currentBucket;
        } else {
            currentBucket = alignmentsRemain.get(0).getStart()/alignmentBucketSize;
        }


        //1º, 2º, 3º
        List<Alignment>[] alignmentBuckets = splitIntoAlignmentBuckets(alignmentRegion);

        //4º
        AlignmentRegionSummary summary = createSummary(alignmentBuckets);
        putSummary(summary);

        //5º Create Proto
        for(List<Alignment> bucket : alignmentBuckets){

            if ((int) currentBucket != bucketsOverlappedStart) {
                System.out.println("we found a buckets gap, fast forward to current bucket, skipping " + ((int)currentBucket-bucketsOverlappedStart) + " overlaps");
            }
            while ((int) currentBucket != bucketsOverlappedStart) {
                numBucketsOverlapped.remove(0);
                bucketsOverlappedStart++;
            }
            //assert((int) currentBucket == bucketsOverlappedStart);  //TODO jj: Replace
            int overlapped = numBucketsOverlapped.remove((int)(currentBucket - bucketsOverlappedStart));

            bucketsOverlappedStart++;
            putBucket(AlignmentProtoHelper.toAlignmentBucketProto(bucket, summary, currentBucket * alignmentBucketSize, overlapped), currentBucket);
            currentBucket++;
        }


        //6º Write into hbase
        try {
            System.out.println("Puteamos la tabla. " + puts.size()+ " Pos: " + currentBucket*alignmentBucketSize);
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
        String rowKey = "S_" + chromosome + "_" + summary.getIndex();

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

    private void putBucket(AlignmentProto.AlignmentBucket alignmentBucket, long index){
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

    private List<Alignment>[] splitIntoAlignmentBuckets(AlignmentRegion alignmentRegion){

        //1º Add remaining alignments.
        List<Alignment> alignments = alignmentsRemain;
        alignments.addAll(alignmentRegion.getAlignments());

        //2º Cut alignments from tail.
        alignmentsRemain = new LinkedList<>();
        Alignment alignmentAux = alignments.remove(alignments.size()-1);    //Remove last
        alignmentsRemain.add(0, alignmentAux);
        long firstBucket = alignments.get(0).getStart()/alignmentBucketSize;
        long lastBucket = alignmentAux.getStart()/alignmentBucketSize;

        while(alignments.size() != 0){
            if(alignments.get(alignments.size()-1).getStart()/alignmentBucketSize != lastBucket){
                break;
            } else {
                alignmentsRemain.add(0, alignments.remove(alignments.size() - 1));    //Remove last
            }
        }

        lastBucket = alignments.get(alignments.size() - 1).getStart()/alignmentBucketSize;

        //3º Split in AlignmentBuckets
        List<Alignment>[] alignmentBuckets = new List[(int)(lastBucket - firstBucket + 1)];
        long bucketEnd = (firstBucket+1)*alignmentBucketSize;
        int i = 0;
        alignmentBuckets[i] = new LinkedList<Alignment>();
        for(Alignment alignment : alignments){
            if(alignment.getStart() > bucketEnd){   // last bucket is complete
                i++;
                bucketEnd+=alignmentBucketSize;
                if(alignment.getStart()/alignmentBucketSize != i+firstBucket){
                    System.out.println("Cuidado! hay un bucket null");
                    i = (int)(alignment.getStart()/alignmentBucketSize-firstBucket);
                    bucketEnd = (1+i+firstBucket)*alignmentBucketSize;
                }
                alignmentBuckets[i] = new LinkedList<Alignment>();
            }
            alignmentBuckets[i].add(alignment);
        }
        return alignmentBuckets;
    }

    /**
     * 4º: create summary
     * 4.1: check that not all alignmentBuckets are null, return empty summary otherwise
     * 4.2: foreach bucket: compute overlaps (*)
     * 4.2.1: write in the summary the overlap of this bucket computed in the previous buckets
     * 4.2.2: get the most far-ending alignment in the bucket.
     * 4.2.3: update the array of overlaps in the next buckets.
     *
     * (*) An overlap of 2 in bucket 7 means that some alignment in
     * bucket 7-2=5 is long enough to end in bucket 7.
     *
     * @param alignmentBuckets array of buckets (bucket == list<Alignment>).
     * @return
     */
    private AlignmentRegionSummary createSummary(List<Alignment>[] alignmentBuckets){

        int i = 0;
        // 4.1
        while(alignmentBuckets[i] == null){
            i++;
            if(i > alignmentBuckets.length){
                System.out.println("TODO! FIXME! PAINFULL!! AlignmentRegionHBaseDataWriter.createSummary(List<Alignment>[] alignmentBuckets)");
                AlignmentRegionSummary summary = new AlignmentRegionSummary(summaryIndex);    //Empty Summary
                summary.close();
                return summary;
            }
        }
        long currentBucket = alignmentBuckets[i].get(0).getStart()/alignmentBucketSize; // first bucket not null
        long lastOverlappedPosition = alignmentBuckets[i].get(0).getUnclippedEnd();

        AlignmentRegionSummary summary = new AlignmentRegionSummary(summaryIndex);

        //4.2
        System.out.println("creating summary for: alignmentBuckets.length " + alignmentBuckets.length);
        for(List<Alignment> bucket : alignmentBuckets){
            // 4.2.1
            if(numBucketsOverlapped.size() <= (int)(currentBucket - bucketsOverlappedStart)) {
                if (numBucketsOverlapped.size() != (int)(currentBucket - bucketsOverlappedStart)) {
                    System.out.println("currentBucket , bucketsOverlappedStart" + currentBucket + " " + bucketsOverlappedStart);
                    System.out.println("size, index" + numBucketsOverlapped.size() + " " + (int)(currentBucket - bucketsOverlappedStart));
                }
                //assert numBucketsOverlapped.size() == (int)(currentBucket - bucketsOverlappedStart);
                while (numBucketsOverlapped.size() <= (int)(currentBucket - bucketsOverlappedStart)) {    // FIXME jj: this could go really bad
                    numBucketsOverlapped.add(0);    // if we have skipped some null buckets, fill them with 0 overlap
                }
            }

            summary.addOverlappedBucket(numBucketsOverlapped.get((int) (currentBucket - bucketsOverlappedStart)));
            //System.out.println("num = " +numBucketsOverlapped.get((int)(currentBucket - bucketsOverlappedStart)) );

            // 4.2.2
            lastOverlappedPosition = 0;
            if(bucket != null){
                for(Alignment alignment : bucket){
                    summary.addAlignment(alignment);
                    lastOverlappedPosition = ((lastOverlappedPosition > alignment.getUnclippedEnd())
                            ? lastOverlappedPosition : alignment.getUnclippedEnd()); // max
                }
            } else {
                lastOverlappedPosition = currentBucket*alignmentBucketSize;
            }

            // 4.2.3
            for (i = 0; i <= lastOverlappedPosition/alignmentBucketSize - currentBucket; i++) { // write bucket overlaps
                if(numBucketsOverlapped.size() > (currentBucket + i - bucketsOverlappedStart)){
                    int previousOverlap = numBucketsOverlapped.get((int) (currentBucket + i- bucketsOverlappedStart));    // get overlap already stored
                    if (previousOverlap < i) {
                        numBucketsOverlapped.set((int)( currentBucket + i - bucketsOverlappedStart), i);
                    }
                } else {
                    numBucketsOverlapped.add(i);
                }
            }
            currentBucket++;
        }

        summary.close();
        summaryIndex++;
        return summary;
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
