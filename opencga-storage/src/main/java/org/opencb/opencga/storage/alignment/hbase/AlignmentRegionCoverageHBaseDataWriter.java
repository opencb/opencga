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
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.alignment.proto.AlignmentProto;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 2/20/14
 * Time: 4:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentRegionCoverageHBaseDataWriter implements DataWriter<AlignmentRegion> {

    private final HBaseManager hBaseManager;
    private HTable table;
    
    private final String tableName;
    private final String sample;
    private String columnFamilyName = AlignmentHBase.ALIGNMENT_COVERAGE_COLUMN_FAMILY_NAME;
    
    private List<Put> puts;
    private int bucketSize = AlignmentHBase.ALIGNMENT_BUCKET_SIZE;
    
    private AlignmentProto.Coverage.Builder coverageBuilder;
    private long coverageStart;
    private long coverageEnd;
    private String chromosome = "";
    
    private int a, c, g, t;  //Counters for null values in coverage.
    

    public AlignmentRegionCoverageHBaseDataWriter(Configuration config, String tableName, String sampleName) {
        this.puts = new LinkedList<>();
        this.tableName = tableName;
        this.sample = sampleName;
        this.hBaseManager = new HBaseManager(config);
    }
    public AlignmentRegionCoverageHBaseDataWriter(Properties props, String tableName, String sampleName) {
        this.puts = new LinkedList<>();
        this.tableName = tableName;
        this.sample = sampleName;
        this.hBaseManager = new HBaseManager(props);
    }

//    public AlignmentRegionCoverageHBaseDataWriter(MonbaseCredentials credentials, String tableName) {
//        // HBase configuration
//        config = HBaseConfiguration.create();
//        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
//        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
//        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
//
//        this.puts = new LinkedList<>();
//        this.tableName = tableName;
//    }

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
        return true;
    }

    @Override
    public boolean post() {
        if(coverageBuilder != null){
            putCoverage();
        }
        try {
            table.put(puts);
            puts.clear();
            return true;
        } catch (InterruptedIOException ex) {
            Logger.getLogger(AlignmentRegionCoverageHBaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RetriesExhaustedWithDetailsException ex) {
            Logger.getLogger(AlignmentRegionCoverageHBaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    

    @Override
    public boolean write(AlignmentRegion alignmentRegion) {
        //System.out.println("Write " + alignmentRegion.getStart() + "Elements " + alignmentRegion.getCoverage().getAll().length);
        RegionCoverage regionCoverage = alignmentRegion.getCoverage();
        
        
        if(coverageBuilder == null){
            this.initBuilder(regionCoverage, 0);
        }
        //Check if it's the same range that
        if(regionCoverage.getStart() > coverageEnd || !regionCoverage.getChromosome().equals(chromosome)){
            this.putCoverage();
            this.initBuilder(regionCoverage, 0);
        }
        
        //write RegionCoverage per nucleotide
        for(int i = 0; i < regionCoverage.getAll().length; i++){
            if(regionCoverage.getStart()+i > coverageEnd){
                this.putCoverage();
                this.initBuilder(regionCoverage, i);
            }
            
            if (regionCoverage.getA()[i] != 0) {
                coverageBuilder.addA(a);
                coverageBuilder.addA(regionCoverage.getA()[i]);
                a = 0;
            } else {
                a++;
            }
            if (regionCoverage.getC()[i] != 0) {
                coverageBuilder.addC(c);
                coverageBuilder.addC(regionCoverage.getC()[i]);
                c=0;
            } else {
                c++;
            }
            if (regionCoverage.getG()[i] != 0) {
                coverageBuilder.addG(g);
                coverageBuilder.addG(regionCoverage.getG()[i]);
                g=0;
            } else {
                g++;
            }if (regionCoverage.getT()[i] != 0) {
                coverageBuilder.addT(t);
                coverageBuilder.addT(regionCoverage.getT()[i]);
                t=0;
            } else {
                t++;
            }
            
            
            
//            coverageBuilder.addA  (regionCoverage.getA()[i]);
//            coverageBuilder.addC  (regionCoverage.getC()[i]);
//            coverageBuilder.addG  (regionCoverage.getG()[i]);
//            coverageBuilder.addT  (regionCoverage.getT()[i]);
            
            coverageBuilder.addAll(regionCoverage.getAll()[i]);
            
        }

        //write mean coverage
        for(MeanCoverage meanCoverage : alignmentRegion.getMeanCoverage()){
            String coverageName = meanCoverage.getName();
            float[] coverage = meanCoverage.getCoverage();

            for(int i = 0; i < coverage.length; i++){
                
                Put put = new Put(Bytes.toBytes(AlignmentHBase.getMeanCoverageRowKey(chromosome, coverageName, (meanCoverage.getInitPosition()/meanCoverage.getSize()) + i)));
                String columnName = sample;
                byte[] value = AlignmentProto.MeanCoverage.newBuilder().setCoverage(coverage[i]).build().toByteArray();
                put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), value);
                puts.add(put);
//                System.out.println("value.length = " + value.length);
//                try {
//                    float l;
//                    System.out.println("Snappy(value).length = " + (l=Snappy.compress(value).length));
//                    System.out.println("value/snappy = " + value.length/l);
//                } catch (IOException ex) {
//                    Logger.getLogger(AlignmentRegionCoverageHBaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
//                }

            }
        }

       // System.out.println("End Write");

        try {
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

       // System.out.println("Table.Puts");
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

    private void initBuilder(RegionCoverage regionCoverage, int offset) {
        coverageBuilder = AlignmentProto.Coverage.newBuilder();
        coverageStart = ((regionCoverage.getStart() + offset) / bucketSize) * bucketSize;
        coverageEnd = coverageStart + bucketSize;
        chromosome = regionCoverage.getChromosome();
        a = c = g = t = 0;
    }

    private void putCoverage() {
        byte[] value;
        Put put = new Put(Bytes.toBytes(AlignmentHBase.getBucketRowkey(chromosome, coverageStart, bucketSize)));
        put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sample), value = coverageBuilder.build().toByteArray());
        puts.add(put);
        
//        System.out.println("value.length = " + value.length);
//        try {
//            float l;
//            System.out.println("Snappy(value).length = " + (l = Snappy.compress(value).length));
//            System.out.println("value/snappy = " + value.length / l);
//        } catch (IOException ex) {
//            Logger.getLogger(AlignmentRegionCoverageHBaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
//        }
        
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
