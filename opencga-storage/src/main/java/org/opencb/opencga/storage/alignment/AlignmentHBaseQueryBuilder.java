package org.opencb.opencga.storage.alignment;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.opencb.opencga.storage.datamanagers.HBaseManager;

import java.io.IOException;

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
/*
        manager.connect();

        HTable table = manager.createTable(tableName, columnFamilyName);

        QueryResult<Alignment> queryResult = new QueryResult<>();

        String startRow = region.getChromosome() + "_" + String.format("%07d", region.getStart() >> 8);
        String endRow = region.getChromosome() + "_" + String.format("%07d", region.getEnd() >> 8);

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));

        ResultScanner resultScanner;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            System.err.println("[ERROR] -- Bad query");
            return null;
        }

        for(Result result : resultScanner){
            for(KeyValue keyValue : result.list()){
                //System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));

                AlignmentProto.AlignmentRegion alignmentRegion = null;
                try {
                    alignmentRegion = AlignmentProto.AlignmentRegion.parseFrom(keyValue.getValue());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                }
                long pos = AlignmentProtoHelper.getPositionFromRowkey(Bytes.toString(keyValue.getRow()));
                for(AlignmentProto.AlignmentRecord alignmentRecord : alignmentRegion.getAlignmentRecordsList()){
                    Alignment alignment = AlignmentProtoHelper.toAlignment(alignmentRecord, region.getChromosome(), (int) pos);
                    queryResult.addResult(alignment);
                }
            }
        }

        manager.disconnect();
        return queryResult;

    */
        return null;
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
