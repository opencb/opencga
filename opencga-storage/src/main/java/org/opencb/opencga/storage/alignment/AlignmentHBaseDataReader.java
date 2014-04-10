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
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.opencb.opencga.storage.datamanagers.HBaseManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 6:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentHBaseDataReader implements DataReader<Alignment> {

    private HBaseManager hBaseManager;
    String tableName, columnFamilyName;
    HTable table;
    private Iterator<KeyValue> keyValueIterator = null;
    private Iterator<AlignmentProto.AlignmentRecord> alignmentRecordIterator = null;
    private Iterator<Result> resultIterator = null;
    private long position = 0;

    private int bucketSize = 256;   //FIXME jj: HARDCODE

    private KeyValue keyValue;

    private AlignmentProto.Header protoHeader;

    public AlignmentHBaseDataReader(Configuration config, String tableName) {
        hBaseManager = new HBaseManager(config);
        this.tableName = tableName;
    }

    public AlignmentHBaseDataReader(MonbaseCredentials credentials, String tableName) {
        hBaseManager = new HBaseManager(credentials);
        this.tableName = tableName;
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

        table = hBaseManager.createTable(tableName, columnFamilyName);

        ResultScanner headerResult;
        try {
            String startRow = "header";
            String endRow = "";   //All starting from "header"
            Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
            headerResult = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        for(Result result : headerResult){
            for(KeyValue keyValue : result.list()){
                try {
                    AlignmentProto.Header.Region region = AlignmentProto.Header.Region.parseFrom(keyValue.getValue());
                    System.out.println(Bytes.toString(keyValue.getRow()) + " " + region.getChromosomeName() + " " + (region.getMaxValue()*256));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public Alignment read() {
        Alignment alignment = null;
        int state;

        if(alignmentRecordIterator == null || !alignmentRecordIterator.hasNext()){
            if(keyValueIterator == null || !keyValueIterator.hasNext()){
                if(resultIterator == null || !resultIterator.hasNext()){
                    state = 0;
                } else {
                    state = 1;
                }
            } else {    //Read from the AlignmentRegion
                state = 2;
            }
        } else {
            state = 3;
        }

        switch(state) {     // There are NO breaks.
            case 0:     //Have to read from HBase
                try {
                    System.out.println("Not Implemented!"); //TODO jj:
                    String startRow = "";
                    String endRow = "";
                    Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
                    resultIterator = table.getScanner(scan).iterator();
                    //TODO jj: If there are no results, read again!
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            case 1:     //Take another result
                if(resultIterator.hasNext()){
                    keyValueIterator = resultIterator.next().list().iterator();
                }
            case 2:     //Parse another AlignmentProto.AlignmentRegion from byte[]
                if(keyValueIterator.hasNext()){
                    try {
                        alignmentRecordIterator = AlignmentProto.AlignmentBucket.parseFrom((keyValue = keyValueIterator.next()).getValue()).getAlignmentRecordsList().iterator();
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            case 3:     //ProtoToAlignment
                String chromosome = AlignmentProtoHelper.getChromosomeFromRowkey(Bytes.toString(keyValue.getRow()));
                long pos = AlignmentProtoHelper.getPositionFromRowkey(Bytes.toString(keyValue.getRow()), bucketSize);
                //alignment = AlignmentProtoHelper.toAlignment(alignmentRecordIterator.next(), chromosome, (int)pos);
                //TODO jj: Adapt to new schema
        }

        return alignment;
    }

    @Override
    public List<Alignment> read(int batchSize) {    //TODO jj: Check
        List<Alignment> alignmentList = new LinkedList<>();
//        for(int i = 0; i < batchSize; i++){
//            alignmentRegionList.add(read());
//        }
        Alignment alignment;
        for(int i = 0; i < batchSize; i++){
            alignment = read();
            if(alignment != null){
                alignmentList.add(alignment);
            }
        }
        return alignmentList;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
