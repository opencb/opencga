package org.opencb.opencga.storage.alignment.hbase;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.opencb.opencga.storage.alignment.proto.AlignmentProto;
import org.opencb.opencga.storage.alignment.proto.AlignmentProtoHelper;
import org.opencb.opencga.storage.alignment.AlignmentQueryBuilder;
import org.opencb.opencga.storage.alignment.AlignmentRegionSummary;
import org.opencb.opencga.storage.datamanagers.HBaseManager;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public AlignmentHBaseQueryBuilder(MonbaseCredentials credentials, String tableName) {
        manager = new HBaseManager(credentials);
        this.tableName = tableName;
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

        HTable table = manager.createTable(tableName, columnFamilyName);

        QueryResult<Alignment> queryResult = new QueryResult<>();

        int bucketSize = 256;   //FIXME: HARDCODE!
        String startRow = region.getChromosome() + "_" + String.format("%07d", region.getStart() / bucketSize);
        String endRow = region.getChromosome() + "_" + String.format("%07d", region.getEnd() / bucketSize);

        System.out.println("Scaning from " + startRow + " to " + endRow);
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));

        ResultScanner resultScanner;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            System.err.println("[ERROR] -- Bad query");
            return null;
        }

        Map<Integer, AlignmentRegionSummary> summaryMap = new HashMap<>();

        for(Result result : resultScanner){
            for(KeyValue keyValue : result.list()){
                //System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : "/* + Bytes.toString(keyValue.getValue())*/);

                AlignmentProto.AlignmentBucket alignmentBucket = null;
                try {
                    alignmentBucket = AlignmentProto.AlignmentBucket.parseFrom(Snappy.uncompress(keyValue.getValue()));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                }

                //System.out.println("Tenemos un bucket!");
                AlignmentRegionSummary summary;
                if(!summaryMap.containsKey(alignmentBucket.getSummaryIndex())) {
                    summaryMap.put(alignmentBucket.getSummaryIndex(), getRegionSummary(region.getChromosome(), alignmentBucket.getSummaryIndex(), table));
                }
                summary = summaryMap.get(alignmentBucket.getSummaryIndex());


                long pos = AlignmentProtoHelper.getPositionFromRowkey(Bytes.toString(keyValue.getRow()), bucketSize);
                List<Alignment> alignmentList = AlignmentProtoHelper.fromAlignmentBucketProto(alignmentBucket, summary, region.getChromosome(), pos);

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

    private AlignmentRegionSummary getRegionSummary(String chromosome, int index,HTable table){
       // manager.connect();

       // HTable table = manager.createTable(tableName, columnFamilyName);

        Scan scan = new Scan(
                Bytes.toBytes(AlignmentProtoHelper.getSummaryRowkey(chromosome, index)) ,
                Bytes.toBytes(AlignmentProtoHelper.getSummaryRowkey(chromosome, index+1)));

        ResultScanner resultScanner;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            System.err.println("[ERROR] -- Bad query");
            return null;
        }
        AlignmentRegionSummary summary = null;
        for(Result result : resultScanner){
            for(KeyValue keyValue : result.list()){
                //System.out.println("Qualifier : " + keyValue.getKeyString() );
                try {
                    summary = new AlignmentRegionSummary( AlignmentProto.Summary.parseFrom(Snappy.uncompress(keyValue.getValue())), index);
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

        throw new UnsupportedOperationException("Not supported yet.");
        //QueryResult<Alignment> queryResult = new QueryResult<>();
        //return queryResult;
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
