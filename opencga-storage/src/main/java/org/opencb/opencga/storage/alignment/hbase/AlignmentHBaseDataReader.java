package org.opencb.opencga.storage.alignment.hbase;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.opencga.storage.alignment.AlignmentSummary;
import org.opencb.opencga.storage.alignment.proto.AlignmentProto;
import org.opencb.opencga.storage.alignment.proto.AlignmentProtoHelper;
import org.xerial.snappy.Snappy;


/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 3/6/14
 * Time: 6:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentHBaseDataReader implements AlignmentDataReader<Alignment> {

    private final HBaseManager hBaseManager;
    private final String  tableName, sampleName;
    private HTable table;


    private Region region;
    private boolean regionScannerSet = false;  //If region is set, only one scanner may be created.
    
    private int bucketSize;// = AlignmentHBase.ALIGNMENT_BUCKET_SIZE;
    private boolean snappyCompress;
    private final String columnFamilyName = AlignmentHBase.ALIGNMENT_COLUMN_FAMILY_NAME;

    
    private AlignmentHBaseHeader hbHeader;
    private AlignmentHeader header;
    private Iterator<AlignmentHeader.SequenceRecord> sequenceDiccionaryIterator;

    public AlignmentHBaseDataReader(Configuration config, String tableName, String sampleName) {
        hBaseManager = new HBaseManager(config);
        this.tableName = tableName;
        this.sampleName = sampleName;
    }

    public AlignmentHBaseDataReader(Properties pro, String tableName, String sampleName) {
        hBaseManager = new HBaseManager(pro);
        this.tableName = tableName;
        this.sampleName = sampleName;
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

        Result result;
        try {
            String startRow = AlignmentHBase.getHeaderRowKey();
            Get get = new Get(Bytes.toBytes(startRow));
            get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sampleName));
            
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        Cell cell = result.listCells().get(0);
        try {
            String json = new String(CellUtil.cloneValue(cell));
            ObjectMap o = new ObjectMap();
            Map<String, AlignmentHBaseHeader> map = o.getJsonObjectMapper().readValue(json, new TypeReference<Map<String, AlignmentHBaseHeader>>() { });
            this.hbHeader = map.get("header");
            this.header = this.hbHeader.getHeader();
            this.bucketSize = hbHeader.getBucketSize();
            this.snappyCompress = hbHeader.isSnappyCompress();
        } catch (IOException ex) {
            Logger.getLogger(AlignmentHBaseDataReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        return getNewScanner();
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public List<Alignment> read(){
        return Arrays.asList(readElem());
    }
    private List<Alignment> readAlignments = new LinkedList<>();
    private String lastRowKey;
    private AlignmentSummary summary;
    private String chromosome = "0";
    private Region regionLimit = null;
    public ResultScanner scanner;
    
    public Alignment readElem() {
        
        if(readAlignments.isEmpty()){
            
            Result result;
            try {
                result = scanner.next();
            } catch (IOException ex) {
                scanner.close();
                return null;
            }
            if(result == null){
                if (getNewScanner()) {
                    return readElem();
                } else {
                    return null;
                }
            }
            for(Cell cell : result.listCells()){
                AlignmentProto.AlignmentBucket bucket;
                lastRowKey = new String(CellUtil.cloneRow(cell));
                String newChromosome = AlignmentHBase.getChromosomeFromRowkey(lastRowKey);
                if (!newChromosome.equals(chromosome)) {
                    scanner.close();
                    if (getNewScanner()) {
                        return readElem();
                    } else {
                        return null;
                    }
                }
                
                
                try {
                    byte[] value = CellUtil.cloneValue(cell);
                    byte[] uncompress;
                    if(snappyCompress){
                        uncompress = Snappy.uncompress(value);
                    } else {
                        uncompress = value;
                    }                   
                    bucket = AlignmentProto.AlignmentBucket.parseFrom(uncompress);
                } catch (InvalidProtocolBufferException ex) {
                    Logger.getLogger(AlignmentHBaseDataReader.class.getName()).log(Level.SEVERE, null, ex);
                    return null;
                } catch (IOException ex) {
                    Logger.getLogger(AlignmentHBaseDataReader.class.getName()).log(Level.SEVERE, null, ex);
                    return null;
                }
                int index = bucket.getSummaryIndex();
                

                if(summary == null || summary.getIndex() != index) {
                    getSummary(index);
                }

                List<Alignment> alignmentList = AlignmentProtoHelper.toAlignmentList(bucket, summary, chromosome, AlignmentHBase.getPositionFromRowkey(lastRowKey, bucketSize));
                readAlignments.addAll(alignmentList);
            }   
        }
        if(readAlignments.isEmpty()){
            return readElem();
        } else {
            Alignment ret = readAlignments.remove(0);
            if(region != null){
                if(ret.getStart() > region.getEnd() /*|| ret.getStart() < region.getStart()*/){
                    ret = null; //If it's out of bounds, return null.
                                //Don't need to test chromosome, already tested.
                }
            }
            return ret;
        }
    }
    private boolean getNewScanner(){
        if(sequenceDiccionaryIterator == null){
            sequenceDiccionaryIterator = header.getSequenceDiccionary().iterator();
        }
        if (sequenceDiccionaryIterator.hasNext()){
            int startPosition = 0;
            if(scanner != null){
                scanner.close();
            }
            if (region != null) {
                if(regionScannerSet){
                    return false;
                } else {
                    chromosome = region.getChromosome();
                    startPosition = region.getStart();
                    regionScannerSet = true;
                }
            } else {
                chromosome = sequenceDiccionaryIterator.next().getSequenceName();
            }
            summary = null;
            String bucketRowkey = AlignmentHBase.getBucketRowkey(chromosome, startPosition, bucketSize);
            
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(bucketRowkey));
            scan.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sampleName));
            //scan.setMaxVersions(); test this

            try {
                scanner = table.getScanner(scan);
            } catch (IOException ex) {
                Logger.getLogger(AlignmentHBaseDataReader.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public List<Alignment> read(int batchSize) {    //TODO jj: Check
        List<Alignment> alignmentList = new LinkedList<>();
//        for(int i = 0; i < batchSize; i++){
//            alignmentRegionList.add(read());
//        }
        Alignment alignment;
        for(int i = 0; i < batchSize; i++){
            alignment = readElem();
            if(alignment != null){
                alignmentList.add(alignment);
            }
        }
        return alignmentList;
    }

    @Override
    public AlignmentHeader getHeader() {
        return header;
    }

    private void getSummary(int summaryIndex) {
        Result result;
        Get get;
        String rowKey = AlignmentHBase.getSummaryRowkey(chromosome, summaryIndex);
        try {
            get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(sampleName));
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("[ERROR] Fail to get Summary from HBase : " + rowKey);
        }

        for (Cell cell : result.listCells()) {
            try {
                byte[] value = CellUtil.cloneValue(cell);
                byte[] uncompress;
                if(snappyCompress){
                    uncompress = Snappy.uncompress(value);
                } else {
                    uncompress = value;
                }
                AlignmentProto.Summary summary = AlignmentProto.Summary.parseFrom(uncompress);
                this.summary = new AlignmentSummary(summary, summaryIndex);
                return;
            } catch (InvalidProtocolBufferException ex) {
                Logger.getLogger(AlignmentHBaseDataReader.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException("[ERROR] Fail to decode Summary from proto : " + rowKey);
            } catch (IOException ex) {
                Logger.getLogger(AlignmentHBaseDataReader.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException("[ERROR] Missing Summary : " + rowKey);
            }
        }
    }
    
    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public String getTableName() {
        return tableName;
    }
    
    public void setRegion(Region region){
        this.region = region;
    }

    

}
