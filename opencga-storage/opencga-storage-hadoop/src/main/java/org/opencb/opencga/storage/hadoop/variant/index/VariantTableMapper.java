/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.stream.Collectors;

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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.opencga.storage.core.StudyConfiguration;
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
    private StudyConfiguration studyConfiguration = null;
    private final Map<Integer, VcfMeta> vcfMetaMap = new ConcurrentHashMap<>();
    private Connection dbConnection = null;
    
    public Logger getLog() {
        return LOG;
    }

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        getLog().debug("Setup configuration");
        // Setup configuration
        helper.set(loadHelper(context));
        this.studyConfiguration = loadStudyConf(); // Variant meta

        // Open DB connection
        dbConnection = ConnectionFactory.createConnection(context.getConfiguration());

        // Load VCF meta data for columns
        initVcfMetaMap(context.getConfiguration()); // Archive meta

        super.setup(context);
    }
    
    public StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

    @Override
    protected void cleanup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        if (null != this.dbConnection) {
            dbConnection.close();
        }
    }

    protected StudyConfiguration loadStudyConf() throws IOException {
        return getHelper().loadMeta();
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
        byte[] intputTable = getHelper().getIntputTable();
        getLog().debug("Load VcfMETA from " + Bytes.toString(intputTable));
        TableName tname = TableName.valueOf(intputTable);
        Connection con = getDbConnection();
        try (
                Table table = con.getTable(tname);
        ) {
            Get get = new Get(getHelper().getMetaRowKey());
            // Don't limit for only specific columns - will be needed in case of hole filling!!!
            // TODO test if really all columns are needed
            Result res = table.get(get);
            NavigableMap<byte[], byte[]> map = res.getFamilyMap(getHelper().getColumnFamily());
            for(Entry<byte[], byte[]> e : map.entrySet()){
                Integer id = Bytes.toInt(e.getKey()); // file ID
                vcfMetaMap.put(id, VcfMeta.parseFrom(e.getValue()));
            }
        }
        getLog().info(String.format("Loaded %s VcfMETA data!!!", vcfMetaMap.size()));
    }

    public VariantTableHelper getHelper() {
        return helper.get();
    }

    protected Connection getDbConnection() {
        return dbConnection;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, 
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        // TODO count and look for reference blocks if there is one variant
        if(value.isEmpty()){
            context.getCounter("OPENCGA.HBASE", "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }
        try{
            if(Bytes.equals(key.get(), getHelper().getMetaRowKey()))
                return; // ignore metadata column

            context.getCounter("OPENCGA.HBASE", "VCF_BLOCK_READ").increment(1);

            // Calculate various positions
            String blockId = Bytes.toString(key.get());
            VariantTableHelper h = getHelper();
            String chr = h.extractChromosomeFromBlockId(blockId );
            Long sliceReg = h.extractPositionFromBlockId(blockId);
            Long startPos = h.getStartPositionFromSlice(sliceReg);
            Long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);
            String nextSliceKey = h.generateBlockId(chr, nextStartPos);

            // Load Archive data (selection only
            Map<Integer, List<Pair<List<Genotype>, Variant>>> position2GtVariantMap = 
                    unpack(value, context,startPos.intValue(), nextStartPos.intValue(),false);

            Set<Integer> positionWithVariant = filterForVariant(position2GtVariantMap);

            // Load Variant data (For study)
            Map<Integer, Map<String, Result>> vartableMap = loadCurrentVariantsRegion(context,key.get(), nextSliceKey);

            Set<Integer> archPosMissing = new HashSet<Integer>(vartableMap.keySet());
            archPosMissing.removeAll(position2GtVariantMap.keySet());
            if(!archPosMissing.isEmpty()){
                // should never happen - positions exist in variant table but not in archive table
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-error-FIXME").increment(1);
                getLog().error(
                        String.format("Positions found in variant table but not in Archive table: %s", 
                                Arrays.toString(archPosMissing.toArray(new Integer[0]))));
            }

            // Positions already in VAR table -> update stats / add other variants
            Set<Integer> varPosUpdate = new HashSet<Integer>(positionWithVariant);
            varPosUpdate.retainAll(vartableMap.keySet());
            
            Map<String, VariantTableStudyRow> updatedVar = merge(context,
                            filterByPosition(position2GtVariantMap,varPosUpdate),
                            translate(filterByPosition(vartableMap,varPosUpdate)));

            // Missing positions in VAR table -> require Archive table fetch of all columns
            Set<Integer> varPosMissing = new HashSet<Integer>(positionWithVariant);
            varPosMissing.removeAll(vartableMap.keySet());
            // TODO Archive table - fetch all columns instead of just the current once
            Map<Integer,Collection<Variant>> archMissing = 
                    filterByPosition(vartableMap, varPosMissing).entrySet().stream()
                    .collect(Collectors.toMap(
                            p -> p.getKey(), 
                            p -> ((List<Pair<List<Genotype>, Variant>>)p.getValue()).stream().map(
                                    s -> s.getSecond()).collect(Collectors.toList())));
            
            Map<String,VariantTableStudyRow> newVar = createNewVar(context,archMissing);
            
            // merge output
            updatedVar.putAll(newVar);
//                    fetchCurrentValues(context, summary.keySet());
            updateOutputTable(context,updatedVar);
        }catch(InvalidProtocolBufferException e){
            throw new IOException(e);
        }
    }

    private Map<String, VariantTableStudyRow> createNewVar(
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context,
            Map<Integer, Collection<Variant>> archMissing) {
        Map<String, VariantTableStudyRow> newVar = new HashMap<String, VariantTableStudyRow>();
        for(Entry<Integer, Collection<Variant>> entry : archMissing.entrySet()){
            Integer pos = entry.getKey();
            // find SNV/SNP variants 
            List<Variant> varSnvp = entry.getValue().stream()
                    .filter(v -> (v.getType().equals(VariantType.SNV) || v.getType().equals(VariantType.SNP)) && v.getStart().equals(pos))
                    .collect(Collectors.toList());
            
            if(varSnvp.isEmpty()){
                continue;
            }
            
            // TODO -> use all variants, not only the first!!!
            Variant chosenVar = varSnvp.get(0);
            VariantTableStudyRow row = new VariantTableStudyRow(
                    getHelper().getStudyId(), chosenVar.getChromosome(), 
                    chosenVar.getStart().longValue(), chosenVar.getReference(), chosenVar.getAlternate());
            String currRowKey = row.generateRowKey(getHelper());
            
            for(Variant var : entry.getValue()){
                String rowKey = getHelper().generateVcfRowId(var.getChromosome(), pos.longValue(), var.getReference(), var.getAlternate());
                Map<String, List<Integer>> gtToSampleIds = createGenotypeIndex(var);
                if(!currRowKey.equals(rowKey)){
                    context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new-multi-alleleic").increment(1);
                    row.addHomeRefCount(gtToSampleIds.get(VariantTableStudyRow.HOM_REF).size());// add to 0/0
                    // update as other
                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.OTHER));
                    continue;
                } 
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new").increment(1);
                // update -> should be default
                row.addHomeRefCount(gtToSampleIds.get(VariantTableStudyRow.HOM_REF).size());// add to 0/0
                row.addSampleId(VariantTableStudyRow.HET_REF, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
                row.addSampleId(VariantTableStudyRow.HOM_VAR, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
                row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.OTHER));
            }
            newVar.put(currRowKey, row);
        }
        
        return newVar;
    }

    private Map<String, VariantTableStudyRow> merge(
            Context context, Map<Integer, List<Pair<List<Genotype>, Variant>>> inArch,
            Map<Integer, Map<String, VariantTableStudyRow>> rowMap) throws InvalidProtocolBufferException {
        Map<String, VariantTableStudyRow> resMap = new HashMap<String, VariantTableStudyRow>();
        // Update rows
        for(Entry<Integer, List<Pair<List<Genotype>, Variant>>> entry : inArch.entrySet()){
            Integer pos = entry.getKey();
            Map<String, VariantTableStudyRow> varMap = rowMap.get(pos); //get variant row
            if(varMap.size() > 1){
                // ignore for the moment TODO implement for >1 variant on same position
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-merge-ignore-multi-allelic").increment(1);
                continue;
            }
            /* TODO -> update count of variants in the same position, but different variant
             * -> currently these counts are only update for the first variant, other variants are just ignored.
             */
            String varRowKey = varMap.keySet().stream().findFirst().get();
            // in case to fill counts for other rows - keep a copy of the first entry
//            VariantTableStudyRow firstRow = new VariantTableStudyRow(varMap.get(varRowKey));
            
            for(Pair<List<Genotype>, Variant> pair : entry.getValue()){
                Variant var = pair.getSecond();
                Map<String, List<Integer>> gtToSampleIds = createGenotypeIndex(var);
                String rowKey = getHelper().generateVcfRowId(var.getChromosome(), pos.longValue(), var.getReference(), var.getAlternate());
                if(!StringUtils.equalsIgnoreCase(rowKey, varRowKey)){
                    // ignore different variants at the same position
                    // just update statistics
                    VariantTableStudyRow row = varMap.get(varRowKey);
                    context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-merge-ignore-multi-allelic_update").increment(1);
                    row.addHomeRefCount(gtToSampleIds.get(VariantTableStudyRow.HOM_REF).size());// add to 0/0
                    // update as other
                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.OTHER));
                    continue;
                }
                
                VariantTableStudyRow row = varMap.get(rowKey);
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-update").increment(1);
                // update -> should be default
                row.addHomeRefCount(gtToSampleIds.get(VariantTableStudyRow.HOM_REF).size());// add to 0/0
                row.addSampleId(VariantTableStudyRow.HET_REF, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
                row.addSampleId(VariantTableStudyRow.HOM_VAR, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
                row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.OTHER));

                resMap.put(rowKey, row);

//                if(row == null){  // not sure if this works TODO test
//                    context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-null").increment(1);
//                    row = new VariantTableStudyRow(
//                            getHelper().getStudyId(), var.getChromosome(),pos.longValue(),var.getReference(),var.getAlternate());
//                    row.setHomRefCount(anyRow.getHomRefCount()); //always the same
//                    // move  0/1 1/1 to 'other'
//                    row.addSampleId(VariantTableStudyRow.OTHER, row.getSampleIds(VariantTableStudyRow.HET_REF));
//                    row.addSampleId(VariantTableStudyRow.OTHER, row.getSampleIds(VariantTableStudyRow.HOM_VAR));
//
//                    // check 'other' for current 0/1 1/1 
//                    Set<Integer> other = row.getSampleIds(VariantTableStudyRow.OTHER);
//                    other.removeAll(gtToSampleIds.get(VariantTableStudyRow.HET_REF));
//                    other.removeAll(gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
//                    
//                    // update
//                    row.addHomeRefCount(gtToSampleIds.get(VariantTableStudyRow.HOM_REF).size());// add to 0/0
//                    row.addSampleId(VariantTableStudyRow.HET_REF, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
//                    row.addSampleId(VariantTableStudyRow.HOM_VAR, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
//                    row.addSampleId(VariantTableStudyRow.OTHER, other);
//                }
            }
        }
        return resMap;
    }

    private Map<String, List<Integer>> createGenotypeIndex(Variant var) {
        StudyEntry se = var.getStudies().get(0);
        int gtpos = se.getFormatPositions().get("GT");
        List<List<String>> sdList = se.getSamplesData();
        
        // init
        Map<String, List<Integer>> gtToSampleIds = new HashMap<String, List<Integer>>();
        gtToSampleIds.put(VariantTableStudyRow.HOM_REF, new ArrayList<Integer>());
        gtToSampleIds.put(VariantTableStudyRow.HET_REF, new ArrayList<Integer>());
        gtToSampleIds.put(VariantTableStudyRow.HOM_VAR, new ArrayList<Integer>());
        gtToSampleIds.put(VariantTableStudyRow.OTHER, new ArrayList<Integer>());
        
        // create GT - SampleId index
        for(Entry<String, Integer> sample2pos : se.getSamplesPosition().entrySet()){
            Integer sampleId = getStudyConfiguration().getSampleIds().get(sample2pos.getKey());
            String gt= sdList.get(
                    sample2pos.getValue())
                    .get(gtpos);
            // TODO Only consider 0/0, 0/1, 1/1 and ? (for others e.g. 1/2 ) for the moment.
            switch (gt) {
            case VariantTableStudyRow.HOM_REF:
            case VariantTableStudyRow.HET_REF:
            case VariantTableStudyRow.HOM_VAR:
                break;
            default:
                gt = VariantTableStudyRow.OTHER;
                break;
            }
            List<Integer> list = gtToSampleIds.get(gt);
            if(list == null){
                list = new ArrayList<Integer>();
                gtToSampleIds.put(gt, list);
            }
            list.add(sampleId);
        }
        return gtToSampleIds;
    }

    private Map<Integer, Map<String, VariantTableStudyRow>> translate(Map<Integer, Map<String, Result>> map) {
        // Translate to other object
        Map<Integer, Map<String, VariantTableStudyRow>> rowMap = new HashMap<Integer, Map<String,VariantTableStudyRow>>();
        for(Entry<Integer, Map<String, Result>> resMap : map.entrySet()){
            Map<String, VariantTableStudyRow> studyRowMap = resMap.getValue().entrySet().stream()
            .collect(Collectors.toMap(
                    p -> p.getKey(), 
                    p -> new VariantTableStudyRow(getHelper().getStudyId(), p.getValue(), getHelper())));
            rowMap.put(resMap.getKey(), studyRowMap);
        }
        return rowMap;
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

    private <T> Map<Integer,T> filterByPosition(Map<Integer,T> map,Set<Integer> filter){
        return map.entrySet().stream()
                .filter(p -> filter.contains(p.getKey()))
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
    }

    private Set<Integer> filterForVariant(Map<Integer, List<Pair<List<Genotype>, Variant>>> position2GtVariantMap) {
        Set<Integer> posList = new HashSet<Integer>();
        for(Entry<Integer, List<Pair<List<Genotype>, Variant>>> entry : position2GtVariantMap.entrySet()){
            Integer pos = entry.getKey();
            for(Pair<List<Genotype>, Variant> pair : entry.getValue()){
                Variant var = pair.getSecond();
                List<Genotype> gtlst = pair.getFirst();
                VariantType type = var.getType();
                if(type.equals(VariantType.NO_VARIATION)){
                    continue; // no variant
                } else if(type.equals(VariantType.SNV) || type.equals(VariantType.SNP)){
                    if(hasVariant(gtlst)){
                        posList.add(pos);
                        break; // no need to search further
                    }
                } else {
                    continue; // ignore for the moment TODO for later
                }
            }
        }
        return posList;
    }
    
    private boolean hasVariant(Collection<Genotype> genotypeColl){
        // TODO add the other variants as well later
        for(Genotype gt : genotypeColl){
            if(gt.isAllelesRefs() ){
                continue; // allele ref
            } else if(gt.getCode().equals(AllelesCode.ALLELES_MISSING)){
                continue; // missing alleles
            } else if(gt.getCode().equals(AllelesCode.MULTIPLE_ALTERNATES)){
                continue; // e.g. 1/2
            } else{
                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @param value   {@link Result} object from Archive table
     * @param context
     * @param sliceStartPos 
     * @param nextSliceStartPos 
     * @param reload  TRUE, if values are reloaded from Archive table on demand to fill gaps - otherwise FALSE
     * @return
     * @throws IOException 
     */
    private Map<Integer, List<Pair<List<Genotype>, Variant>>> unpack(Result value, Context context, int sliceStartPos, 
            int nextSliceStartPos, boolean reload) throws IOException {
        Map<Integer, List<Pair<List<Genotype>, Variant>>> resMap = new HashMap<Integer, List<Pair<List<Genotype>,Variant>>>();
        NavigableMap<byte[], byte[]> fm = value.getFamilyMap(getHelper().getColumnFamily());
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            int fileId = Bytes.toInt(x.getKey());
            VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(this.getVcfMeta(fileId));
            VcfSlice vcfSlice = asSlice(x.getValue());
            context.getCounter("OPENCGA.HBASE", "VCF_SLICE_READ").increment(1);
            
            List<Variant> varList = converter.convert(vcfSlice);
            int minStartPos = addVariants(context, varList, resMap, sliceStartPos, nextSliceStartPos, reload);
            if(minStartPos > sliceStartPos){ // more information in previous region
                context.getCounter("OPENCGA.HBASE", "VCF_SLICE-break-region").increment(1);
//                querySliceBreak(context,resMap,previousArchiveSliceRK(value.getRow()),x.getKey(),sliceStartPos,nextSliceStartPos);
            }
        }
        return resMap;
    }

    /**
     * Query slice break for the previous slice
     * @param context
     * @param resMap
     * @param row
     * @param col
     * @param sliceStartPos
     * @param nextSliceStartPos
     * @throws IOException
     */
    private void querySliceBreak(Context context,Map<Integer, List<Pair<List<String>, Variant>>> resMap, 
            byte[] row, byte[] col, int sliceStartPos, int nextSliceStartPos) throws IOException {
        Get get = new Get(row);
        get.addColumn(getHelper().getColumnFamily(), col);
        Result res = getHelper().getHBaseManager().act(getDbConnection(),getHelper().getIntputTable(), table -> {return table.get(get);});
        // TODO check if sufficient 
    }

    private byte[] previousArchiveSliceRK(byte[] currRk){
        return null; //TODO
    }

    private int addVariants(Context context, List<Variant> varList, Map<Integer, List<Pair<List<Genotype>, Variant>>> resMap, 
            int sliceStartPos, int nextSliceStartPos, boolean reload) {
        int minStartPos = nextSliceStartPos;
        for(Variant var : varList){
            VariantType vtype = var.getType();
            if(! reload){
                logCount(context, vtype); // update Count for output
            }
            List<Genotype> gtLst = extractGts(var);
            Pair<List<Genotype>, Variant> pair = new Pair<List<Genotype>, Variant>(gtLst,var);
            int start = Math.max(sliceStartPos, var.getStart()); // get max start pos (in case of backwards search)
            int end = Math.min(nextSliceStartPos, var.getEnd()); // Don't go over the edge of the slice
            // One entry per position in region
            for(int vcfPos = start; vcfPos <= end; ++vcfPos){
                List<Pair<List<Genotype>, Variant>> list = resMap.get(vcfPos);
                if(list == null){
                    list = new ArrayList<Pair<List<Genotype>, Variant>>();
                    resMap.put(vcfPos, list);
                }
                list.add(pair);
            }
            minStartPos = Math.min(minStartPos,start); // update overall min
        }
        return minStartPos;
    }

    private List<Genotype> extractGts(Variant var) {
        StudyEntry se = var.getStudies().get(0);
        int gtpos = se.getFormatPositions().get("GT");
        List<Genotype> gtList = new ArrayList<Genotype>();
        for (List<String> sd : se.getSamplesData()) {
            String gt = sd.get(gtpos);
            Genotype gtobj = new Genotype(gt, var.getReference(), var.getAlternate());
            gtList.add(gtobj);
        }
        return gtList;
    }

    private void logCount(Context ctx, VariantType vtype) {
        ctx.getCounter("OPENCGA.HBASE", "VCF_Record_Count").increment(1);
        switch (vtype) {
        case NO_VARIATION:
            ctx.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-no-variant    ").increment(1);
            break;
        case SNV:
        case SNP:
            ctx.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-variant").increment(1);
            break;
        case MNV:
        case MNP:
        case SYMBOLIC:
        case MIXED:
        case INDEL:
        case SV:
        case INSERTION:
        case DELETION:
        case TRANSLOCATION:
        case INVERSION:
        case CNV:
            ctx.getCounter("OPENCGA.HBASE", "VCF_REC_CALL-ignore").increment(1);
            break;
        default:
            break;
        }
    }

    protected Map<Integer, Map<String, Result>> loadCurrentVariantsRegion(Context context, byte[] sliceKey, String endKey) throws IOException {
        Map<Integer, Map<String, Result>> resMap = new HashMap<Integer, Map<String, Result>>();
        try (
                Table table = getDbConnection().getTable(TableName.valueOf(getHelper().getOutputTable()));
        ) {
            VariantTableHelper h = getHelper();
            Scan scan = new Scan(sliceKey, Bytes.toBytes(endKey));
            scan.setFilter(new PrefixFilter(sliceKey));
            scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(getHelper().getStudyId() + "_")));
            ResultScanner rs = table.getScanner(scan); 
            for(Result r : rs){
                String rowStr = Bytes.toString(r.getRow());
                Long pos = h.extractPositionFromVariantRowkey(rowStr);
                context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-result").increment(1);
                if(!r.isEmpty()){ // only non empty rows
                    Map<String, Result> res = resMap.get(pos.intValue());
                    if(null == res){
                        res = new HashMap<String, Result>();
                        resMap.put(pos.intValue(), res);
                    }
                    res.put(rowStr, r);
                }
            }
        }
        return resMap ;
    }

    /**
     * Load (if available) current data, merge information and store new object in DB
     * @param context
     * @param variants 
     * @param summary
     * @param currRes 
     * @throws IOException 
     * @throws InterruptedException 
     */
    private void updateOutputTable(Context context, Map<String, VariantTableStudyRow> variants) 
            throws IOException, InterruptedException {
        for(VariantTableStudyRow row : variants.values()){
            Put put = row.createPut(getHelper());
            context.write(new ImmutableBytesWritable(put.getRow()), put);
            context.getCounter("OPENCGA.HBASE", "VCF_ROW-put").increment(1);
            
        }
    }

    private VcfMeta getVcfMeta(Integer id) {
        return this.vcfMetaMap.get(id);
    }

    private VcfSlice asSlice(byte[] data) throws InvalidProtocolBufferException {
        return VcfSlice.parseFrom(data);
    }

}
