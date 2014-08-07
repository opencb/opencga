package org.opencb.opencga.storage.alignment.hbase;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.opencb.opencga.storage.alignment.AlignmentQueryBuilder;
import org.opencb.opencga.storage.alignment.AlignmentSummary;
import org.opencb.opencga.storage.alignment.proto.AlignmentProto;
import org.opencb.opencga.storage.alignment.proto.AlignmentProtoHelper;
import org.xerial.snappy.Snappy;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/7/14
 * Time: 10:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentHBaseQueryBuilder implements AlignmentQueryBuilder {

    HBaseManager manager;
    String tableName, columnFamilyName = null;

    public void connect(){
        manager.connect();
    }

//    public AlignmentHBaseQueryBuilder(MonbaseCredentials credentials, String tableName) {
//        manager = new HBaseManager(credentials);
//        this.tableName = tableName;
//    }
    
    public AlignmentHBaseQueryBuilder(String tableName) {

    }
    
    public AlignmentHBaseQueryBuilder(Configuration config, String tableName) {
        manager = new HBaseManager(config);
        this.tableName = tableName;
    }

    @Override
    public QueryResult getAllAlignmentsByRegion(Region region, QueryOptions options) {
        boolean wasOpened = true;
        if(!manager.isOpened()){
            manager.connect();
            wasOpened = false;
        }

        HTable table = manager.getTable(tableName);//manager.createTable(tableName, columnFamilyName);
        if(table == null){
            return null;
        }
        QueryResult<Alignment> queryResult = new QueryResult<>();

        String sample = options.getString("sample", "HG00096");
        String family = options.getString("family", "c");
        
        
        int bucketSize = 256;   //FIXME: HARDCODE!
        //String startRow = region.getChromosome() + "_" + String.format("%07d", region.getStart() / bucketSize);
        //String endRow = region.getChromosome() + "_" + String.format("%07d", region.getEnd() / bucketSize);
        
        String startRow = AlignmentHBase.getBucketRowkey(region.getChromosome(), region.getStart(), bucketSize);
        String endRow = AlignmentHBase.getBucketRowkey(region.getChromosome(), region.getEnd()+bucketSize, bucketSize);
        
        System.out.println("Scaning from " + startRow + " to " + endRow);
        Scan scan = new Scan();
        
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(sample));
        scan.setMaxVersions(1);
//        scan.setMaxResultSize()
        
        ResultScanner resultScanner;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            System.err.println("[ERROR] -- Bad query");
            return null;
        }

        Map<Integer, AlignmentSummary> summaryMap = new HashMap<>();

        for(Result result : resultScanner){
            for(Cell cell : result.listCells()){
                //System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : "/* + Bytes.toString(keyValue.getValue())*/);
                
                AlignmentProto.AlignmentBucket alignmentBucket = null;
                try {
                    alignmentBucket = AlignmentProto.AlignmentBucket.parseFrom(Snappy.uncompress(CellUtil.cloneValue(cell)));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                }

                //System.out.println("Tenemos un bucket!");
                AlignmentSummary summary;
                if(!summaryMap.containsKey(alignmentBucket.getSummaryIndex())) {
                    summaryMap.put(alignmentBucket.getSummaryIndex(), getRegionSummary(region.getChromosome(), alignmentBucket.getSummaryIndex(), table));
                }
                summary = summaryMap.get(alignmentBucket.getSummaryIndex());


                long pos = AlignmentHBase.getPositionFromRowkey(org.apache.hadoop.hbase.util.Bytes.toString(cell.getRowArray()), bucketSize);
                List<Alignment> alignmentList = AlignmentProtoHelper.toAlignmentList(alignmentBucket, summary, region.getChromosome(), pos);

                //System.out.println("Los tenemos!!");

                for(Alignment alignment : alignmentList){
                    queryResult.addResult(alignment);
                }
            }
        }

        if(!wasOpened){
            manager.disconnect();
        }
        return queryResult;


        //return null;
    }

    private AlignmentSummary getRegionSummary(String chromosome, int index,HTable table){
       // manager.connect();

       // HTable table = manager.createTable(tableName, columnFamilyName);

        Scan scan = new Scan(
                Bytes.toBytes(AlignmentHBase.getSummaryRowkey(chromosome, index)) ,
                Bytes.toBytes(AlignmentHBase.getSummaryRowkey(chromosome, index+1)));

        ResultScanner resultScanner;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            System.err.println("[ERROR] -- Bad query");
            return null;
        }
        AlignmentSummary summary = null;
        for(Result result : resultScanner){
            for(KeyValue keyValue : result.list()){
                //System.out.println("Qualifier : " + keyValue.getKeyString() );
                try {
                    summary = new AlignmentSummary( AlignmentProto.Summary.parseFrom(Snappy.uncompress(keyValue.getValue())), index);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
        return summary;
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {

        QueryResult<RegionCoverage> queryResult = new QueryResult<>();
        
        if (!manager.isOpened()) {
            manager.connect();
        }
        
        String sample =  options.getString("sample", "HG00096");
        String family =  options.getString("family", "c");

        int bucketSize = 256;   //FIXME: HARDCODE!
        //String startRow = region.getChromosome() + "_" + String.format("%07d", region.getStart() / bucketSize);
        //String endRow = region.getChromosome() + "_" + String.format("%07d", region.getEnd() / bucketSize);

        String startRow = AlignmentHBase.getBucketRowkey(region.getChromosome(), region.getStart(), bucketSize);
        String endRow = AlignmentHBase.getBucketRowkey(region.getChromosome(), region.getEnd() + bucketSize, bucketSize);

        HTable table = manager.getTable(tableName);
        
        Scan scan = new Scan();
        
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(sample));
        scan.setMaxVersions(1);
        
        
        ResultScanner scanner;
        try {
            scanner = table.getScanner(scan);
        } catch (IOException ex) {
            Logger.getLogger(AlignmentHBaseQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                AlignmentProto.Coverage parseFrom;
                try { 
                    parseFrom = AlignmentProto.Coverage.parseFrom(CellUtil.cloneValue(cell));
                } catch (InvalidProtocolBufferException ex) {
                    Logger.getLogger(AlignmentHBaseQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return queryResult;
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
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
