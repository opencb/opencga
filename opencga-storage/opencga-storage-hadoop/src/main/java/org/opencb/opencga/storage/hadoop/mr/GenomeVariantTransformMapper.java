/**
 *
 */
package org.opencb.opencga.storage.hadoop.mr;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.commons.logging.LogFactory;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSample;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.opencga.storage.hadoop.models.variantcall.protobuf.VariantCallMeta;
import org.opencb.opencga.storage.hadoop.models.variantcall.protobuf.VariantCallProtos.VariantCallProt;
import org.opencb.opencga.storage.hadoop.models.variantcall.protobuf.VariantCallProtos.VariantCallProt.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeVariantTransformMapper extends TableMapper<ImmutableBytesWritable, Put>{
    private static final String _TABLE_COUNT_COLUMN = "0/0";

    private final Logger LOG = LoggerFactory.getLogger(GenomeVariantTransformDriver.class);

    private final AtomicReference<GenomeVariantTransformHelper> helper = new AtomicReference<>();
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
        meta.set(loadMeta());

        // Load VCF meta data for columns
        initVcfMetaMap(context.getConfiguration());
        super.setup(context);
    }

    protected VariantCallMeta loadMeta() throws IOException {
        return new VariantCallMeta(getHelper().loadMeta());
    }

    protected GenomeVariantTransformHelper loadHelper(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) {
        return new GenomeVariantTransformHelper(context.getConfiguration());
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
        getLog().debug(String.format("Loaded %s VcfMETA data!!!", vcfMetaMap.size()));
    }

    public GenomeVariantTransformHelper getHelper() {
        return helper.get();
    }

    public VariantCallMeta getMeta() {
        return meta.get();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {

        Map<String, Map<String, List<Integer>>> summary = new HashMap<>();
        try {

            String blockId = Bytes.toString(key.get());

            GenomeVariantTransformHelper h = getHelper();
            String chr = h.extractChromosomeFromBlockId(blockId);
//            Long sliceReg = h.extractPositionFromBlockId(blockId);
//            Long startPos = h.getStartPositionFromSlice(sliceReg);
//            Long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);

            NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(h.getColumnFamily());
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                Integer id = getMeta().getIdFromColumn(Bytes.toString(entry.getKey()));
                VcfSlice vcfSlice = asSlice(entry.getValue());
                List<VcfRecord> records = vcfSlice.getRecordsList();
                for (VcfRecord record : records) {
                    int vcfPos = vcfSlice.getPosition() + record.getRelativeStart();

                    String row_key = generateKey(chr, vcfPos, record.getReference(), record.getAlternate(0));
                    String gt = extractGt(id, record);
                    updateSummary(summary, id, row_key, gt);
                }
            }
            Map<String, Result> currRes = fetchCurrentValues(summary.keySet());
            updateOutputTable(context, summary, currRes);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    protected Map<String, Result> fetchCurrentValues(Set<String> keySet) throws IOException {
        //TODO: Reuse a connection
        return getHelper().getHBaseManager().act(getHelper().getOutputTable(), table -> {
            Map<String, Result> resMap = new HashMap<>();
            List<Get> gets = new ArrayList<>(keySet.size());
            for(String rowKey : keySet){
                byte[] rkBytes = Bytes.toBytes(rowKey);
                gets.add(new Get(rkBytes));
            }
            Result[] results = table.get(gets);
            for (Result result : results) {
                resMap.put(Bytes.toString(result.getRow()), result);
            }
            return resMap ;
        });
    }

    /**
     * Load (if available) current data, merge information and store new object in DB
     * @param context
     * @param summary
     * @param currentResults
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
            if(res.isEmpty()){
                context.write(new ImmutableBytesWritable(rkBytes), put);
                context.getCounter("OPENCGA.HBASE", "VCF_ROW_NEW").increment(1);
            } else {
                Put mergedPut = mergeData(res,put);
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
    private Put mergeData(Result res, Put put) throws IOException {
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
            Builder b = VariantCallProt.newBuilder();
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

    private String extractGt(Integer id, VcfRecord rec) {
        int gtIdx = findGtIndex(id,rec);
        if(rec.getSamplesCount() != 1)
            throw new NotImplementedException("Only one Sample per VCF record supported at the moment");
        VcfSample sample = rec.getSamples(0);
        return sample.getSampleValues(gtIdx);
    }

    private int findGtIndex(Integer id, VcfRecord rec) {
        if(rec.getSampleFormatNonDefaultCount() > 0){
            return findIndex("GT", rec.getSampleFormatNonDefaultList());
        }
        VcfMeta meta = getVcfMeta(id);
        return findIndex("GT", meta.getFormatDefaultList());
    }

    private int findIndex(String str,List<String> list){
        int idx = list.indexOf(str);
        if(idx < 0)
            throw new IllegalStateException(String.format("String %s not found in index!!!",str));
        return idx;
    }

    private VcfMeta getVcfMeta(Integer id) {
        return this.vcfMetaMap.get(id);
    }

    private VcfSlice asSlice(byte[] data) throws InvalidProtocolBufferException {
        return VcfSlice.parseFrom(data);
    }


}
