/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.alignment.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.auth.MonbaseCredentials;
import org.opencb.opencga.storage.alignment.AlignmentSummary;
import org.opencb.opencga.storage.alignment.AlignmentSummary.AlignmentRegionSummaryBuilder;
import org.opencb.opencga.storage.alignment.proto.AlignmentProto;
import org.opencb.opencga.storage.alignment.proto.AlignmentProtoHelper;
import org.xerial.snappy.Snappy;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 4:48 PM
 */
public class AlignmentRegionHBaseDataWriter implements DataWriter<AlignmentRegion> {

    private final HBaseManager hBaseManager;
    private final String tableName;
    private final AlignmentDataReader reader;
    private HTable table;
    private final String sample;
    private List<Put> puts;

    private AlignmentHeader header;

    private int alignmentBucketSize = AlignmentHBase.ALIGNMENT_BUCKET_SIZE;
    private String columnFamilyName = AlignmentHBase.ALIGNMENT_COLUMN_FAMILY_NAME;

    //
    List<Alignment> alignmentsRemain = new LinkedList<>();
    private int summaryIndex = 0;
    private String chromosome = "";

    // alignments overlapped along several buckets
    private LinkedList<Integer> numBucketsOverlapped = new LinkedList<>();
    private long bucketsOverlappedStart = 0;

    private boolean snappyCompress = false;
    
    //Summary Fields
    private long summarySpace = 0;
    private long alignmentsSpace = 0;
    private long bucketsWritten = 0;
    
    
    public AlignmentRegionHBaseDataWriter(Properties props, String tableName, String sampleName, AlignmentDataReader reader) {
        hBaseManager = new HBaseManager(props);
        this.puts = new LinkedList<>();
        this.tableName = tableName;
        this.reader = reader;
        this.sample = sampleName;
    }
    public AlignmentRegionHBaseDataWriter(Configuration config, String tableName, String sampleName, AlignmentDataReader reader) {
        hBaseManager = new HBaseManager(config);
        this.puts = new LinkedList<>();
        this.tableName = tableName;
        this.reader = reader;
        this.sample = sampleName;
    }

    @Override
    public boolean open() {
        return hBaseManager.connect();

    }

    @Override
    public boolean close() {

        return hBaseManager.disconnect();

    }

    @Override
    public boolean pre() {
        table = hBaseManager.createTable(tableName,columnFamilyName);   //Creates or get table
        header = reader.getHeader();
        writeGlobalHeader(header);
        return true;
    }

    @Override
    public boolean post() {
        try {
            //System.out.println("Puteamos la tabla. " + puts.size());
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        System.out.println("---------------HBase Storage-----------------");
        System.out.println("Amoung space stored in Summaries : " + summarySpace);
        System.out.println("Amoung space stored in Alignments: " + alignmentsSpace);
        System.out.println("Buckets written : " + this.bucketsWritten);
        System.out.println("AlignmentBucketSize : " + this.alignmentBucketSize);
        System.out.println("Snappy compress : " + this.snappyCompress);
        return true;
    }

    @Override
    public boolean write(AlignmentRegion alignmentRegion) {

        /*
         * 1º Add remaining alignments from last AR
         * 2º Cut Alignments from tail
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
                AlignmentSummary summary = createSummary(list);
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
        AlignmentSummary summary = createSummary(alignmentBuckets);
        putSummary(summary);

        //5º Create Proto
        for(List<Alignment> bucket : alignmentBuckets){
            int overlapped = numBucketsOverlapped.remove((int)(currentBucket - bucketsOverlappedStart));
            assert((int) currentBucket - bucketsOverlappedStart == 0);  //TODO jj: Replace
            bucketsOverlappedStart++;
            putBucket(AlignmentProtoHelper.toAlignmentBucketProto(bucket, summary, currentBucket * alignmentBucketSize, overlapped), currentBucket);
            currentBucket++;
        }


        //6º Write into hbase
        try {
            //System.out.println("Puteamos la tabla. " + puts.size()+ " Pos: " + currentBucket*alignmentBucketSize);
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        return true;
    }

    private void writeGlobalHeader(AlignmentHeader header) { 
        
        String rowKey = AlignmentHBase.getHeaderRowKey();
        Put put = new Put(Bytes.toBytes(rowKey));
        AlignmentHBaseHeader hbHeader= new AlignmentHBaseHeader(header, alignmentBucketSize, snappyCompress);
        ObjectMap h = new ObjectMap("header", hbHeader);
        
        String headerString = h.toJson();
        
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), Bytes.toBytes(headerString));
        
        try {
            table.put(put);
            return;
        } catch (InterruptedIOException ex) {
            Logger.getLogger(AlignmentRegionHBaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RetriesExhaustedWithDetailsException ex) {
            Logger.getLogger(AlignmentRegionHBaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        throw new RuntimeException("[ERROR] while pushing the AlignmentHeader to HBase");
    }


    private void putSummary(AlignmentSummary summary){
        String rowKey = AlignmentHBase.getSummaryRowkey(chromosome, summary.getIndex());

        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] compress;
        try {
            AlignmentProto.Summary toProto = summary.toProto();
            compress = toProto.toByteArray();
            if(snappyCompress){
                compress = Snappy.compress(compress);
            }
            summarySpace+=compress.length;
        } catch (IOException e) {
            e.printStackTrace(); 
            throw new RuntimeException("[ERROR] this AlignmentProto.Summary " + rowKey + " could not be compressed by snappy");
        }
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), compress);

        puts.add(put);
    }

    private void putBucket(AlignmentProto.AlignmentBucket alignmentBucket, long index){
        if(alignmentBucket == null)
            return;

        String rowKey = AlignmentHBase.getBucketRowkey(chromosome,index);
        //System.out.println("Creamos un Put() con rowKey " + rowKey);

        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] compress = alignmentBucket.toByteArray();
        try {
            if(snappyCompress){
                compress = Snappy.compress(compress);
            }
            alignmentsSpace+=compress.length;
        } catch (IOException e) {
            e.printStackTrace();  
            throw new RuntimeException("[ERROR] this AlignmentProto.AlignmentBucket " + rowKey + " could not be compressed by snappy");
        }
        bucketsWritten++;
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
            if(alignment.getStart() > bucketEnd){
                i++;
                bucketEnd+=alignmentBucketSize;
                if(alignment.getStart()/alignmentBucketSize != i+firstBucket){
                    //System.out.println("Cuidado!");
                    i = (int)(alignment.getStart()/alignmentBucketSize-firstBucket);
                    bucketEnd = (1+i+firstBucket)*alignmentBucketSize;
                }
                alignmentBuckets[i] = new LinkedList<Alignment>();
            }
            alignmentBuckets[i].add(alignment);
        }
        return alignmentBuckets;
    }

    private AlignmentSummary createSummary(List<Alignment>[] alignmentBuckets){

        //4º Create Summary

        AlignmentRegionSummaryBuilder builder = new AlignmentRegionSummaryBuilder(summaryIndex++);

        int i = 0;
        while(alignmentBuckets[i] == null){
            i++;
            if(i > alignmentBuckets.length){
                System.out.println("TODO! FIXME! PAINFULL!! AlignmentRegionHBaseDataWriter.createSummary(List<Alignment>[] alignmentBuckets)");
                return builder.build();//Empty Summary
            }
        }
        long currentBucket = alignmentBuckets[i].get(0).getStart()/alignmentBucketSize;
        long lastOverlappedPosition = alignmentBuckets[0].get(0).getUnclippedEnd();


        for(List<Alignment> bucket : alignmentBuckets){
            //System.out.println(currentBucket + " " + bucketsOverlappedStart);
            builder.addOverlappedBucket(numBucketsOverlapped.get((int)(currentBucket - bucketsOverlappedStart)));

            lastOverlappedPosition = 0;
            if(bucket != null){
                for(Alignment alignment : bucket){
                    builder.addAlignment(alignment);
                    lastOverlappedPosition = ((lastOverlappedPosition > alignment.getUnclippedEnd()) ? lastOverlappedPosition : alignment.getUnclippedEnd()); // max
                }
            } else {
                lastOverlappedPosition = currentBucket*alignmentBucketSize;
            }
            /**
             * Updates the array of overlaps.
             * An overlap of 2 in bucket 7 means that some alignment in
             * bucket 7-2=5 is long enough to end in bucket 7.
             */
            for (i = 0; i <= lastOverlappedPosition/alignmentBucketSize - currentBucket; i++) { // write bucket overlaps
                if(numBucketsOverlapped.size() < (currentBucket + i - bucketsOverlappedStart)){
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

        return builder.build();
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


    public String getTableName() {
        return tableName;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }
}
