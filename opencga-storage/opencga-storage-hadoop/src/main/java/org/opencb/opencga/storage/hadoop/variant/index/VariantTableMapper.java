/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSample;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.index.models.protobuf.VariantCallMeta;
import org.opencb.opencga.storage.hadoop.variant.index.models.protobuf.VariantCallProtos.VariantCallProt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableMapper extends TableMapper<ImmutableBytesWritable, Put>{
    private static final String _TABLE_COUNT_COLUMN = "0/0";

    private final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    private final AtomicReference<VariantTableHelper> helper = new AtomicReference<>();
    private final AtomicReference<VariantCallMeta> meta = new AtomicReference<>();
    private final Map<Integer, VcfMeta> vcfMetaMap = new ConcurrentHashMap<>();
    
    public Logger getLog() {
        return LOG;
    }

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        getLog().debug("Setup configuration");
        // Setup configuration
        helper.set(loadHelper(context));
        meta.set(loadMeta()); // Variant meta
//        HBaseStudyConfigurationManager studyConfigurationManager = new HBaseStudyConfigurationManager(null, context.getConfiguration(), new ObjectMap());
//        studyConfigurationManager.getStudyConfiguration(studyId, options);
        // Load VCF meta data for columns
        initVcfMetaMap(context.getConfiguration()); // Archive meta
        super.setup(context);
    }

    protected VariantCallMeta loadMeta() throws IOException {
        return new VariantCallMeta(getHelper().loadMeta());
    }

    protected VariantTableHelper loadHelper(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) {
        return new VariantTableHelper(context.getConfiguration());
    }

    /**
     * Load VCF Meta data from input table and create table index
     * @param conf
     * @throws IOException
     */
    protected void initVcfMetaMap(Configuration conf) throws IOException {
        // TODO optimize to only required columns
        byte[] intputTable = getHelper().getIntputTable();
        getLog().debug("Load VcfMETA from " + Bytes.toString(intputTable));
        TableName tname = TableName.valueOf(intputTable);
        try (
                Connection con = ConnectionFactory.createConnection(conf);
                Table table = con.getTable(tname);
        ) {
            Get get = new Get(getHelper().getMetaRowKey());
            Result res = table.get(get);
            NavigableMap<byte[], byte[]> map = res.getFamilyMap(getHelper().getColumnFamily());
            for(Entry<byte[], byte[]> e : map.entrySet()){
                Integer id = getMeta().getIdFromColumn(Bytes.toString(e.getKey()));
                vcfMetaMap.put(id, VcfMeta.parseFrom(e.getValue()));
            }
        }
        getLog().info(String.format("Loaded %s VcfMETA data!!!", vcfMetaMap.size()));
    }

    public VariantTableHelper getHelper() {
        return helper.get();
    }
    
    public VariantCallMeta getMeta() {
        return meta.get();
    }
    
    @Override
    protected void map(ImmutableBytesWritable key, Result value, 
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        context.getCounter("OPENCGA.HBASE", "VCF_BLOCK_READ").increment(1);
        // TODO count and look for reference blocks if there is one variant
        Map<String, Map<String, List<Integer>>> summary = new HashMap<>();
        try{
            if(Bytes.equals(key.get(), getHelper().getMetaColumnKey()))
                return; // ignore metadata column
            
            String blockId = Bytes.toString(key.get());
            
            VariantTableHelper h = getHelper();
            String chr = h.extractChromosomeFromBlockId(blockId );
            Long sliceReg = h.extractPositionFromBlockId(blockId);
            Long startPos = h.getStartPositionFromSlice(sliceReg);
            Long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);
            String nextSliceKey = h.generateBlockId(chr, nextStartPos);
            if(value.isEmpty()){
                context.getCounter("OPENCGA.HBASE", "VCF_RESULT_EMPTY").increment(1);
                return;
            }
//            for(Cell cell : value.listCells()){
//                Integer id = getMeta().getIdFromColumn(Bytes.toString(cell.getQualifierArray()));
//                VcfSlice vcfSlice = asSlice(cell.getValueArray());
//            }
            Map<Integer, List<VcfRecordWrapper>> positionMap = unpack(value, context,false);
            
            Map<Integer,Result> vartableMap = loadCurrentVariantsRegion(context,key.get(), nextSliceKey);
            
            
//            NavigableMap<byte[], byte[]> fm = value.getFamilyMap(h.getColumnFamily());
//            for (Entry<byte[], byte[]> x : fm.entrySet()) {
//                Integer id = getMeta().getIdFromColumn(Bytes.toString(x.getKey()));
//                VcfSlice vcfSlice = asSlice(x.getValue());
//                context.getCounter("OPENCGA.HBASE", "VCF_SLICE_READ").increment(1);
//                List<VcfRecord> lst = vcfSlice.getRecordsList();
//                context.getCounter("OPENCGA.HBASE", "VCF_Record_Count").increment(vcfSlice.getRecordsCount());
//                for(VcfRecord rec : lst){
//                    VariantTransformWrapper wrapper = new VariantTransformWrapper(id);
//                    context.getCounter("OPENCGA.HBASE", "VCF_REC_READ").increment(1);
//                    String gt = extractGt(id,rec);
//                    wrapper.setGenotype(gt);
//                    String alternate = rec.getAlternates();
//                    wrapper.setStatus(CallStatus.VARIANT);
//                    if(isReference(gt)){
//                        wrapper.setStatus(CallStatus.REF);
//                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL_REF").increment(1);
//                    } else if(isNoCall(gt)){
//                        wrapper.setStatus(CallStatus.NOCALL);
//                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL_NO").increment(1);
//                    } else if( ! isVariant(gt)){ // should not be called (if not ref or no-call -> should be variant)
//                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL_OTHER-unknown").increment(1);
//                        getLog().warn(String.format("Found Genotype with '%s'",gt));
//                        continue; // TODO monitor if this happens
//                    }   
//                    
//                    int vcfPos = vcfSlice.getPosition() + rec.getRelativeStart();
//                    wrapper.setPosition(vcfPos);
//                    wrapper.setReference(rec.getReference());
//                    wrapper.setChromosome(chr);
//                    
//                    String row_key = generateKey(chr,vcfPos,rec.getReference(),alternate);
//                    context.getCounter("OPENCGA.HBASE", "VCF_REC_UPDATE").increment(1);
//                    updateSummary(summary, id, row_key, gt);
//                }
//            }
            Map<String, Result> currRes = new HashMap<String, Result>();
//                    fetchCurrentValues(context, summary.keySet());
            updateOutputTable(context,summary, currRes);
        }catch(InvalidProtocolBufferException e){
            throw new IOException(e);
        }
    }

    /**
     * 
     * @param value   {@link Result} object from Archive table
     * @param context
     * @param reload  TRUE, if values are reloaded from Archive table on demand to fill gaps - otherwise FALSE
     * @return
     * @throws InvalidProtocolBufferException
     */
    private Map<Integer, List<VcfRecordWrapper>> unpack(Result value, Context context, boolean reload) throws InvalidProtocolBufferException {
        Map<Integer, List<VcfRecordWrapper>> resMap = new HashMap<Integer, List<VcfRecordWrapper>>();
        NavigableMap<byte[], byte[]> fm = value.getFamilyMap(getHelper().getColumnFamily());
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            Integer id = getMeta().getIdFromColumn(Bytes.toString(x.getKey()));
            VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(this.getVcfMeta(id));
            VcfSlice vcfSlice = asSlice(x.getValue());
            
            List<Variant> varList = converter.convert(vcfSlice);
            
            for(Variant var : varList){
                
            }
            
            List<VcfRecord> lst = vcfSlice.getRecordsList();
            if( ! reload){ // count for map-reduce, not for reload
                context.getCounter("OPENCGA.HBASE", "VCF_SLICE_READ").increment(1);
                context.getCounter("OPENCGA.HBASE", "VCF_Record_Count").increment(vcfSlice.getRecordsCount());
            }
            for(VcfRecord rec : lst){
                VcfRecordWrapper wrapper = new VcfRecordWrapper(id, vcfSlice, rec);
                String gt = extractGt(id,rec);
                wrapper.setGenotype(gt);
                if(! reload){ // only show count if Map reduce and not reload
                    context.getCounter("OPENCGA.HBASE", "VCF_REC_READ").increment(1);
                    if(wrapper.isHaploid()){
                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-haploid").increment(1);
                    } else if(wrapper.isMultiAlternates()){
                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-multi-ignore").increment(1);
                    } else if(wrapper.hasNoCall()){
                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-no-call").increment(1);
                    } else if(wrapper.isAllelesRef()){
                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-ref").increment(1);
                    } else if(wrapper.hasVariant()){
                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-variant").increment(1);
                    } else{
                        context.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-unknown-PROBLEM").increment(1);
                        getLog().warn(String.format("Found Genotype with '%s'",gt));
                    }
                }
                int vcfPos = wrapper.getStartPosition();
                List<VcfRecordWrapper> list = resMap.get(vcfPos);
                if(list == null){
                    list = new ArrayList<VcfRecordWrapper>();
                    resMap.put(vcfPos, list);
                }
                list.add(wrapper);
            }
        }
        return resMap;
    }


    protected Map<Integer, Result> loadCurrentVariantsRegion(Context context, byte[] sliceKey, String endKey) throws IOException {
        Map<Integer, Result> resMap = new HashMap<Integer, Result>();
        try (
                Connection con = ConnectionFactory.createConnection(context.getConfiguration());
                Table table = con.getTable(TableName.valueOf(getHelper().getOutputTable()));
        ) {
            VariantTableHelper h = getHelper();
//                String key = generatePositionKey(chr,pos);
//                String ekey = generatePositionKey(chr, pos+1);
//                byte[] rkBytes = Bytes.toBytes(key);
            Scan scan = new Scan(sliceKey, Bytes.toBytes(endKey));
            scan.setFilter(new PrefixFilter(sliceKey));
            ResultScanner rs = table.getScanner(scan); 
            for(Result r : rs){
                Long pos = h.extractPositionFromVariantRowkey(Bytes.toString(r.getRow()));
                context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-result").increment(1);
                if(!r.isEmpty()){ // only non empty rows
                    resMap.put(pos.intValue(), r);
                }
            }
        }
        return resMap ;
    }

    /**
     * Load (if available) current data, merge information and store new object in DB
     * @param context
     * @param summary
     * @param currRes 
     * @throws IOException 
     * @throws InterruptedException 
     */
    private void updateOutputTable(Context context, Map<String, Map<String, List<Integer>>> summary, Map<String, Result> currentResults) 
            throws IOException, InterruptedException {
        for(String rowKey : summary.keySet()){
            byte[] rkBytes = Bytes.toBytes(rowKey);
            // Wrap data
            Put put = wrapData(context, summary, rowKey, rkBytes);
            // Load possible Row
            Result res = currentResults.get(rowKey); 
            if(null == res || res.isEmpty()){
                context.write(new ImmutableBytesWritable(rkBytes), put);
                context.getCounter("OPENCGA.HBASE", "VCF_ROW_NEW").increment(1);
            } else {
                Put mergedPut = mergetData(res,put);
                context.write(new ImmutableBytesWritable(rkBytes), mergedPut);
                context.getCounter("OPENCGA.HBASE", "VCF_ROW_MERGED").increment(1);
            }
        }
    }

    /**
     * Merge Result set with Put object
     * @param res
     * @param put
     * @return Put object with merged data
     * @throws IOException
     */
    private Put mergetData(Result res, Put put) throws IOException {
        byte[] columnFamily = getHelper().getColumnFamily();
        Put nPut = new Put(put.getRow());
        
        NavigableMap<byte[], byte[]> resMap = res.getFamilyMap(columnFamily);
        Set<String> mergedColumns = new HashSet<>();
        for(Entry<byte[], byte[]> e : resMap.entrySet()){
            byte[] colQual = e.getKey();
            mergedColumns.add(Bytes.toString(colQual));
            if(! put.has(columnFamily, colQual)){
                nPut.addColumn(columnFamily, colQual, e.getValue());
            }else {
                byte[] valueArray = put.get(columnFamily, colQual).get(0).getValueArray();
                if(StringUtils.equals(Bytes.toString(colQual), _TABLE_COUNT_COLUMN)){
                    // Update count
                    int dbVal = Bytes.toInt(e.getValue());
                    int putVal = Bytes.toInt(valueArray);
                    int newVal = dbVal + putVal;
                    nPut.addColumn(columnFamily, colQual, Bytes.toBytes(newVal));
                } else {
                    // Merge lists
                    VariantCallProt a = VariantCallProt.parseFrom(e.getValue());
                    VariantCallProt b = VariantCallProt.parseFrom(valueArray);
                    List<Integer> lst = new ArrayList<Integer>(a.getSampleIdsList());
                    lst.addAll(b.getSampleIdsList());
                    Collections.sort(lst);
                    Set<Integer> set = new HashSet<Integer>(lst);
                    if(set.size() < lst.size()) // Check for duplicates!!!
                        throw new IllegalStateException(String.format("Duplicated samples in %s!!!", Arrays.toString(lst.toArray())));
                    byte[] nval = VariantCallProt.newBuilder().addAllSampleIds(lst).build().toByteArray();
                    nPut.addColumn(columnFamily, colQual, nval);
                }
            }
        }
        for(List<Cell> cl : put.getFamilyCellMap().values()){
            for(Cell c : cl){
                if(!mergedColumns.contains(Bytes.toString(c.getQualifierArray()))){
                    nPut.add(c);
                }
            }
        }
        return nPut;
    }

    private Put wrapData(Context context, Map<String, Map<String, List<Integer>>> summary, String rowKey, byte[] rkBytes)
            throws IOException, InterruptedException {
        Put put = new Put(rkBytes);
        for(Entry<String, List<Integer>> e : summary.get(rowKey).entrySet()){
            byte[] col = Bytes.toBytes(e.getKey());
            List<Integer> val = e.getValue();
            // to byte array (count or list of IDs)
            byte[] data = persist(e.getKey(), val);
            put.addColumn(getHelper().getColumnFamily(), col, data);
        }
        return put;
    }

    /**
     * Persist data depending on Column type (count or {@link VariantCallProt} )
     * @param key
     * @param val
     * @return byte[] of data
     */
    private byte[] persist(String key, List<Integer> val) {
        byte[] data = Bytes.toBytes(val.size());
        if(!StringUtils.equals(key, _TABLE_COUNT_COLUMN)){
            Collections.sort(val);
            VariantCallProt.Builder b = VariantCallProt.newBuilder();
            b.addAllSampleIds(val);
            data = b.build().toByteArray();
        }
        return data;
    }

    private void updateSummary(Map<String, Map<String, List<Integer>>> summary, Integer id, String row_key, String gt) {
        Map<String, List<Integer>> rkmap = summary.get(row_key);
        if(null == rkmap){
            rkmap = new HashMap<>();
            summary.put(row_key, rkmap);
        }
        List<Integer> idlst = rkmap.get(gt);
        
        if(null == idlst){
            idlst = new ArrayList<Integer>();
            rkmap.put(gt, idlst);
        }
        idlst.add(id);
    }

    private String generateKey(String chr, int vcfPos, String reference, String alternate) {
        return getHelper().generateVcfRowId(chr, (long) vcfPos, reference, alternate);
    }
    
    private String generatePositionKey(String chrom, int pos){
        return getHelper().generateRowPositionKey(chrom, (long) pos);
    }

    private String extractGt(Integer id, VcfRecord rec) {
        int gtIdx = findGtIndex(id,rec);
        if(rec.getSamplesCount() != 1)
            throw new NotImplementedException("Only one Sample per VCF record supported at the moment");
        VcfSample sample = rec.getSamples(0);
        return sample.getSampleValues(gtIdx);
    }

    private int findGtIndex(Integer id, VcfRecord rec) {
        if(rec.getSampleFormatNonDefaultCount() > 0){
            return findIndex("GT",rec.getSampleFormatNonDefaultList());
        }
        VcfMeta meta = getVcfMeta(id);
        return findIndex("GT",meta.getFormatDefaultList());
    }
    
    private VcfMeta getVcfMeta(Integer id) {
        return this.vcfMetaMap.get(id);
    }

    private int findIndex(String str,List<String> list){
        int idx = list.indexOf(str);
        if(idx < 0)
            throw new IllegalStateException(String.format("String %s not found in index!!!",str));
        return idx;
    }

    private VcfSlice asSlice(byte[] data) throws InvalidProtocolBufferException {
        return VcfSlice.parseFrom(data);
    }
    

}
