/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableMapper extends TableMapper<ImmutableBytesWritable, Put>{
    private static final String _TABLE_COUNT_COLUMN = "0/0";

    private static final int MAX_REVERSE_SEARCH_ITER = 5;

    private static final VariantType[] TARGET_VARIANT_TYPE = new VariantType[]{VariantType.SNV, VariantType.SNP};

    private final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration = null;
    private final Map<Integer, VcfMeta> vcfMetaMap = new ConcurrentHashMap<>();
    private Connection dbConnection = null;
    private List<Long> timeSum = new ArrayList<Long>();
    
    public Logger getLog() {
        return LOG;
    }

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        getLog().debug("Setup configuration");
        // Setup configuration
        helper = loadHelper(context);
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
        for(int i = 0; i < this.timeSum.size(); ++i){
            context.getCounter("OPENCGA.HBASE", "VCF_TIMER_"+i).increment(this.timeSum.get(i));
        }
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
        String tableName = Bytes.toString(getHelper().getIntputTable());
        getLog().debug("Load VcfMETA from {}", tableName);
        try (ArchiveFileMetadataManager metadataManager = new ArchiveFileMetadataManager(tableName, conf, null)) {
            QueryResult<VcfMeta> allVcfMetas = metadataManager.getAllVcfMetas(new ObjectMap());
            for (VcfMeta vcfMeta : allVcfMetas.getResult()) {
                vcfMetaMap.put(Integer.parseInt(vcfMeta.getVariantSource().getFileId()), vcfMeta);
            }
        }
        getLog().info(String.format("Loaded %s VcfMETA data!!!", vcfMetaMap.size()));
    }

    public VariantTableHelper getHelper() {
        return helper;
    }

    protected void setHelper(VariantTableHelper helper){
        this.helper = helper;
    }

    protected Connection getDbConnection() {
        return dbConnection;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, 
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        if(value.isEmpty()){
            context.getCounter("OPENCGA.HBASE", "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }
        try{
            if(Bytes.equals(key.get(), getHelper().getMetaRowKey()))
                return; // ignore metadata column

            context.getCounter("OPENCGA.HBASE", "VCF_BLOCK_READ").increment(1);
            List<Long> times = new ArrayList<Long>();

            times.add(System.currentTimeMillis());

            // Calculate various positions
            byte[] currRowKey = key.get();
            String sliceKey = Bytes.toString(currRowKey);
            VariantTableHelper h = getHelper();
            String chr = h.extractChromosomeFromBlockId(sliceKey );
            Long sliceReg = h.extractSliceFromBlockId(sliceKey);
            Long startPos = h.getStartPositionFromSlice(sliceReg);
            Long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);
            String nextSliceKey = h.generateBlockId(chr, nextStartPos);

// Unpack Archive data (selection only
            List<Variant> archive = 
                    unpack(value, context,startPos.intValue(), nextStartPos.intValue(),false);

            times.add(System.currentTimeMillis());

            Set<Integer> coveredPositions = generateCoveredPositions(archive.stream());

            // Start positions of Variants of specific type
            Set<Integer> archVarStartPositions = 
                    filterForVariant(archive.stream(), TARGET_VARIANT_TYPE)
                        .map(v -> v.getStart()).collect(Collectors.toSet());

            times.add(System.currentTimeMillis());

// Load Variant data (For study) for same region
            Map<Integer, Map<String, Result>> varTabMap = loadCurrentVariantsRegion(context,currRowKey, nextSliceKey);

            times.add(System.currentTimeMillis());

            // Report Missing regions in ARCHIVE table, which are seen in VAR table
            Set<Integer> archPosMissing = new HashSet<Integer>(varTabMap.keySet());
            archPosMissing.removeAll(coveredPositions);
            if(!archPosMissing.isEmpty()){
                // should never happen - positions exist in variant table but not in archive table
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-error-FIXME").increment(1);
                getLog().error(
                        String.format("Positions found in variant table but not in Archive table: %s", 
                                Arrays.toString(archPosMissing.toArray(new Integer[0]))));
            }

            // Find ARCHIVE entries covering all existing positions in the VARIANT table
            List<Variant> varPosUpdateLst = filterByPositionCovered(archive, varTabMap.keySet());

            times.add(System.currentTimeMillis());

// Update VARIANT table entries with archive table entries
            Map<String, VariantTableStudyRow> updatedVar = merge(context, varPosUpdateLst, translate(varTabMap));

            times.add(System.currentTimeMillis());

            // Missing positions in VAR table -> require Archive table fetch of all columns
            Set<Integer> varPosMissing = new HashSet<Integer>(archVarStartPositions);
            varPosMissing.removeAll(varTabMap.keySet());
            // Current 
            List<Variant> archMissingInVar = filterByPositionCovered(archive,varPosMissing);

            times.add(System.currentTimeMillis());

            getLog().debug("Fetch data for missing position from Archive table: " + varPosMissing.size());
            List<Variant> archPreviousFiles = loadArchivePreviousFiles(context, varPosMissing,sliceKey);
            getLog().debug("Fetched Variants from Archive table: " + archPreviousFiles.size());

            times.add(System.currentTimeMillis());

            List<Variant> newTargetVariants = new ArrayList<Variant>(archPreviousFiles);
            newTargetVariants.addAll(archMissingInVar);

            // check that there is no duplication of rows (current mapreduce and reloaded Arch data)
            if(new HashSet<Variant>(newTargetVariants).size() != newTargetVariants.size()){
                throw new IllegalStateException("Double loading of Archive data!!!");
            }

            Map<String,VariantTableStudyRow> newVar = createNewVar(context,newTargetVariants, varPosMissing);

            times.add(System.currentTimeMillis());

            // merge output
            updatedVar.putAll(newVar);
//                    fetchCurrentValues(context, summary.keySet());
            updateOutputTable(context,updatedVar);

            times.add(System.currentTimeMillis());
            addTimes(times);
        }catch(InvalidProtocolBufferException e){
            throw new IOException(e);
        }
    }

    private void addTimes(List<Long> times) {
        int n = times.size();
        boolean init = timeSum.isEmpty();
        for(int i = 1; i < n; ++i){
            long diff = times.get(i) - times.get(i-1);
            if(init)
                timeSum.add(diff);
            else
                timeSum.set(i-1, timeSum.get(i-1) + diff);
        }
    }

    /**
     * Load all registered files for slice from ARCHIVE table and return variants covering the target positions. 
     * Could be memory extensive for lot of files, but there is room for improvement.
     * @param context
     * @param targetPositions Positions of interest
     * @param sliceKey Rowkey
     * @return List of Variants covering the target positions
     * @throws IOException
     */
    private List<Variant> loadArchivePreviousFiles(MapContext context, Set<Integer> targetPositions, String sliceKey) throws IOException {
        // IMPROVEMENT: iterate in blocks of e.g. 200 column to fetch & filter variants, if this is an issue
        // TODO Work out which Columns to Load / ignore !!!!
        List<Variant> var = new ArrayList<Variant>();
        Get get = new Get(Bytes.toBytes(sliceKey));
        BiMap<String, Integer> fileMap = getStudyConfiguration().getFileIds();
        for(Entry<String, Integer> file : fileMap.entrySet()){
            get.addColumn(getHelper().getColumnFamily(), Bytes.toBytes(file.getValue()));
        }
        
        Result res = getHelper().getHBaseManager().act(getDbConnection(),getHelper().getIntputTable(), table -> {return table.get(get);});
        if(res.isEmpty()){
            return var;
        }
        NavigableMap<byte[], byte[]> fm = res.getFamilyMap(getHelper().getColumnFamily());
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            // for each Column extract slice to variants
            List<Variant> archVars = archiveCellToVariants(x.getKey(),x.getValue());
            // Filter Variants on Position covered
            List<Variant> subset = filterByPositionCovered(archVars, targetPositions);
            var.addAll(subset);
        }
        return var;
    }

    protected Set<Integer> generateCoveredPositions(Stream<Variant> variants) {
        return variants.map(v -> generateRegion(v.getStart(),v.getEnd()))
                .flatMap(l -> l.stream()) // hope this works
                .collect(Collectors.toSet());
    }
    
    private Set<Integer> generateRegion(Integer start, Integer end){
        if(end < start){
            throw new IllegalStateException(
                    String.format("End position (%s) is < than Start (%s)!!!", start,end));
        }
        int len = end-start;
        Integer [] array = new Integer[len+1];
        for (int a = 0; a <= len; a++) { // <= to be inclusive
            array[a] = (start + a);
        }
        return new HashSet<Integer>(Arrays.asList(array));
    }

    protected Map<String, VariantTableStudyRow> createNewVar(MapContext context, List<Variant> archMissing, Set<Integer> targetPos) {
        Map<String, VariantTableStudyRow> newVar = new HashMap<String, VariantTableStudyRow>();
        
        Map<Variant,Map<String, List<Integer>>> gtIdx = archMissing.stream().collect(Collectors.toMap(v -> v, v -> createGenotypeIndex(v)));
        Map<Variant, String> rowkeyIdx = archMissing.stream().collect(Collectors.toMap(v -> v,v -> getHelper().generateVariantRowKey(v)));

        for(Integer pos : targetPos){
            List<Variant> varCovingPos = archMissing.stream().filter(v ->  variantCoveringPosition(v, pos)).collect(Collectors.toList());
            List<Variant> varTargetPos = 
                    filterForVariant(varCovingPos.stream(), TARGET_VARIANT_TYPE)
                    .filter(v -> v.getStart().equals(pos))
                    .collect(Collectors.toList());
            
            for(Variant var : varTargetPos){
                VariantTableStudyRow row = new VariantTableStudyRow(
                        getHelper().getStudyId(), var.getChromosome(), var.getStart().longValue(), var.getReference(), var.getAlternate());
                String currRowKey = row.generateRowKey(getHelper());
                if(newVar.containsKey(currRowKey)){
                    context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new-conflict").increment(1);
                    continue; // already dealt with it
                }
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new").increment(1);
                Set<Integer> homRefs = new HashSet<Integer>();
                for(Variant other : varCovingPos){ // also includes the current target  variant
                    Map<String, List<Integer>> gts = gtIdx.get(other);

                    homRefs.addAll(gts.get(VariantTableStudyRow.HOM_REF));
                    if(other.getStart().equals(pos) && StringUtils.equals(rowkeyIdx.get(other), currRowKey)) { // same Variant
                        row.addSampleId(VariantTableStudyRow.HET_REF, gts.get(VariantTableStudyRow.HET_REF));
                        row.addSampleId(VariantTableStudyRow.HOM_VAR, gts.get(VariantTableStudyRow.HOM_VAR));
                        row.addSampleId(VariantTableStudyRow.OTHER, gts.get(VariantTableStudyRow.OTHER));
                    } else{ // different Variant
                        row.addSampleId(VariantTableStudyRow.OTHER, gts.get(VariantTableStudyRow.HET_REF));
                        row.addSampleId(VariantTableStudyRow.OTHER, gts.get(VariantTableStudyRow.HOM_VAR));
                        row.addSampleId(VariantTableStudyRow.OTHER, gts.get(VariantTableStudyRow.OTHER));
                    }
                }

                row.addHomeRefCount(homRefs.size());// add to 0/0
                newVar.put(currRowKey, row);
            }
        }
        return newVar;
    }

    protected Map<String, VariantTableStudyRow> merge(MapContext context, List<Variant> varPosUpdateLst,
            Map<Integer, Map<String, VariantTableStudyRow>> positionMap) throws InvalidProtocolBufferException {

        Map<Variant,Map<String, List<Integer>>> gtIdx = 
                varPosUpdateLst.stream().collect(Collectors.toMap(v -> v, v -> createGenotypeIndex(v)));
        Map<Variant, String> rowkeyIdx = varPosUpdateLst.stream().collect(Collectors.toMap(v -> v,v -> getHelper().generateVariantRowKey(v)));
        
        /* For each position */
        for(Entry<Integer, Map<String, VariantTableStudyRow>> positionEntry : positionMap.entrySet()){
            Integer position = positionEntry.getKey();
            Map<String, VariantTableStudyRow> rowMap = positionEntry.getValue();
            
            /* Find Variants covering this position */
            List<Variant> inPosition = 
                    varPosUpdateLst.stream().filter(v -> variantCoveringPosition(v,position)).collect(Collectors.toList());

            /* For each Row (ref_alt) combination -> update the count*/
            for(Entry<String, VariantTableStudyRow> rowEntry : rowMap.entrySet()) {
                Set<Integer> homRefSet = new HashSet<Integer>();
                VariantTableStudyRow row = rowEntry.getValue();
                
                /* For each Variant */
                for(Variant var : inPosition){
                    // using row key, since this is "start" position specific and not based on the current position (e.g. if region)
                    String rowKey = rowkeyIdx.get(var);
                    Map<String, List<Integer>> gtToSampleIds = gtIdx.get(var);
                    homRefSet.addAll(gtToSampleIds.get(VariantTableStudyRow.HOM_REF));

                    row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.OTHER));
                    if(rowEntry.getKey().equals(rowKey)){ // If same variant
                        context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-var-same").increment(1);
                        row.addSampleId(VariantTableStudyRow.HOM_VAR, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
                        row.addSampleId(VariantTableStudyRow.HET_REF, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
                    } else { // different variant
                        context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-var-different").increment(1);
                        row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
                        row.addSampleId(VariantTableStudyRow.OTHER, gtToSampleIds.get(VariantTableStudyRow.HET_REF));
                    }
                }
                row.addHomeRefCount(homRefSet.size());
            }
        }

        /* For each Variant (SNP/SNV) that does not exist yet */
        Map<String,List<Variant>> variantsWithVar = filterForVariant(varPosUpdateLst.stream(), TARGET_VARIANT_TYPE)
                .collect(Collectors.groupingBy(v -> rowkeyIdx.get(v)));
        for(Entry<String, List<Variant>> varEntry : variantsWithVar.entrySet()){
            String rowKey = varEntry.getKey();
            Variant anyVar = varEntry.getValue().get(0);
            Integer position = anyVar.getStart();
            
            if(!positionMap.containsKey(position)){
                throw new IllegalStateException(
                        String.format("Target variant of different position than exist in variant table!!! %s", anyVar));
            }
            if(positionMap.get(position).containsKey(rowKey)){
                continue; // already exist (rowkey) -> has been updated already
            }

            /* Same position but different variant (e.g. 123_A_T instead of 123_A_G) -> use to copy counts / IDs over */
            VariantTableStudyRow diffVarSampePos = positionMap.get(position).values().stream().findFirst().get();

            VariantTableStudyRow row = 
                    new VariantTableStudyRow(
                            getHelper().getStudyId(), anyVar.getChromosome(), position.longValue(), 
                            anyVar.getReference(), anyVar.getAlternate());

            Map<String, List<Integer>> gtToSampleIds = createGenotypeIndex(varEntry.getValue().toArray(new Variant[0]));

            context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new").increment(1);
            context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new-vars").increment(varEntry.getValue().size());

            /* Transfer full count */
            row.addHomeRefCount(diffVarSampePos.getHomRefCount()); // Same for each position

            /* Move different variants subject IDs to 'OTHER' */
            row.addSampleId(VariantTableStudyRow.OTHER, diffVarSampePos.getSampleIds(VariantTableStudyRow.HOM_VAR));
            row.addSampleId(VariantTableStudyRow.OTHER, diffVarSampePos.getSampleIds(VariantTableStudyRow.HET_REF));

            /* Screen OTHER for sample Ids with current Variant */
            Set<Integer> other = new HashSet<Integer>(diffVarSampePos.getSampleIds(VariantTableStudyRow.OTHER));
            other.removeAll(gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
            other.removeAll(gtToSampleIds.get(VariantTableStudyRow.HET_REF));
            row.addSampleId(VariantTableStudyRow.OTHER, other);

            row.addSampleId(VariantTableStudyRow.HOM_VAR, gtToSampleIds.get(VariantTableStudyRow.HOM_VAR));
            row.addSampleId(VariantTableStudyRow.HET_REF, gtToSampleIds.get(VariantTableStudyRow.HET_REF));

            /* ADD new Variant row */
            positionMap.get(position).put(rowKey, row);
        }
        
        // Transform to output format
        Map<String, VariantTableStudyRow> resMap = 
                positionMap.values().stream()
                    .map(v -> v.values())
                    .flatMap(v -> v.stream())
                    .collect(Collectors.toMap(v -> v.generateRowKey(getHelper()), v -> v));
        return resMap;
    }

    protected boolean variantCoveringPosition(Variant v,Integer position) {
        return variantCoveringRegion(v, position, position, true);
    }
    protected boolean variantCoveringRegion (Variant v, Integer start, Integer end, boolean inclusive) {
        if(inclusive){
            return end >= v.getStart() && start <= v.getEnd();
        } else {
            return end > v.getStart() && start < v.getEnd();
        }
    }

    protected Map<String, List<Integer>> createGenotypeIndex(Variant ... varArr) {
        // init
        Map<String, List<Integer>> gtToSampleIds = new HashMap<String, List<Integer>>();
        gtToSampleIds.put(VariantTableStudyRow.HOM_REF, new ArrayList<Integer>());
        gtToSampleIds.put(VariantTableStudyRow.HET_REF, new ArrayList<Integer>());
        gtToSampleIds.put(VariantTableStudyRow.HOM_VAR, new ArrayList<Integer>());
        gtToSampleIds.put(VariantTableStudyRow.OTHER, new ArrayList<Integer>());
        
        for(Variant var : varArr){
            StudyEntry se = var.getStudies().get(0);
            int gtpos = se.getFormatPositions().get("GT");
            List<List<String>> sdList = se.getSamplesData();
            
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

    private <T> Map<Integer,T> filterByPosition(Map<Integer,T> map,Set<Integer> filter){
        return map.entrySet().stream()
                .filter(p -> filter.contains(p.getKey()))
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
    }
    
    private List<Variant> filterByPositionCovered(List<Variant> vars,Set<Integer> filter){
        return vars.stream()
                .filter(v -> filter.stream().anyMatch(f -> variantCoveringPosition(v,f)))
                .collect(Collectors.toList());
    }

    protected Stream<Variant> filterForVariant(Stream<Variant> variants, VariantType ... types) {
        Set<VariantType> whileList = new HashSet<VariantType>(Arrays.asList(types));
       return variants
                .filter(v -> whileList.contains(v.getType()));
//                .filter(v -> hasVariant(extractGts(v))); // Not needed for GVCF
    }

    protected boolean hasVariant(Collection<Genotype> genotypeColl){
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
    private List<Variant> unpack(Result value, Context context, int sliceStartPos, 
            int nextSliceStartPos, boolean reload) throws IOException {
        List<Variant> variantList = new ArrayList<Variant>();
        NavigableMap<byte[], byte[]> fm = value.getFamilyMap(getHelper().getColumnFamily());
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            // for each FILE (column in HBase
            context.getCounter("OPENCGA.HBASE", "VCF_SLICE_READ").increment(1);
            List<Variant> varList = archiveCellToVariants(x.getKey(),x.getValue());
            context.getCounter("OPENCGA.HBASE", "READ_VARIANTS").increment(varList.size());
            variantList.addAll(varList);
            Integer minStartPos = nextSliceStartPos;
            if(!varList.isEmpty()){
                minStartPos = varList.stream().map(v -> v.getStart()).collect(Collectors.minBy(Comparator.naturalOrder())).get();
            }
            int reverseSearchCnt = 0;
            while(minStartPos >= sliceStartPos && (reverseSearchCnt++) < MAX_REVERSE_SEARCH_ITER){ // more information in previous region
                context.getCounter("OPENCGA.HBASE", "VCF_SLICE-break-region").increment(1);
                List<Variant> var = querySliceBreakPerFile(context,previousArchiveSliceRK(value.getRow()),x.getKey(),sliceStartPos,nextSliceStartPos);
//                context.getCounter("OPENCGA.HBASE", "EXTRA_READ_BLOCKS").increment(1);
//                context.getCounter("OPENCGA.HBASE", "EXTRA_READ_BLOCKS_" + reverseSearchCnt).increment(1);
//                context.getCounter("OPENCGA.HBASE", "EXTRA_READ_VARIANTS").increment(var.size());
                if(!var.isEmpty()){
                    minStartPos = var.stream().map(v -> v.getStart()).collect(Collectors.minBy(Comparator.naturalOrder())).get();
                }
                variantList.addAll(var);
            }
        }
        List<Variant> keepList = variantList.stream() // only keep variants in overlapping this region
                .filter(v -> variantCoveringRegion(v, sliceStartPos, nextSliceStartPos,true))
                .collect(Collectors.toList());
        return keepList;
    }

    /**
     * Query slice break for the previous slice
     * @param context
     * @param row
     * @param col
     * @param sliceStartPos
     * @param nextSliceStartPos
     * @return 
     * @throws IOException
     */
    private List<Variant> querySliceBreakPerFile(Context context,byte[] row, byte[] col, int sliceStartPos, int nextSliceStartPos) throws IOException {
        List<Variant> var = new ArrayList<Variant>();
        Get get = new Get(row);
        get.addColumn(getHelper().getColumnFamily(), col);
        Result res = getHelper().getHBaseManager().act(getDbConnection(),getHelper().getIntputTable(), table -> {return table.get(get);});
        if(res.isEmpty()){
            return var;
        }
        NavigableMap<byte[], byte[]> fm = res.getFamilyMap(getHelper().getColumnFamily());
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            var.addAll(archiveCellToVariants(x.getKey(),x.getValue()));
        }
        return var;
    }

    private List<Variant> archiveCellToVariants(byte[] key, byte[] value) throws InvalidProtocolBufferException {
        int fileId = ArchiveHelper.getFileIdFromColumnName(key);
        //TODO: Reuse this converter
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(this.getVcfMeta(fileId));
        VcfSlice vcfSlice = asSlice(value);
        return converter.convert(vcfSlice);
    }

    private byte[] previousArchiveSliceRK(byte[] currRk){
        String rk = Bytes.toString(currRk);
        String chr = getHelper().extractChromosomeFromBlockId(rk);
        Long slice = getHelper().extractSliceFromBlockId(rk);
        return Bytes.toBytes(getHelper().generateBlockIdFromSlice(chr, slice-1)); 
    }

    private int addVariants(Context context, List<Variant> varList, Map<Integer, List<Variant>> resMap, 
            int sliceStartPos, int nextSliceStartPos, boolean reload) {
        int minStartPos = nextSliceStartPos;
        for(Variant var : varList){
            VariantType vtype = var.getType();
            if(! reload){
                logCount(context, vtype); // update Count for output
            }
            int start = Math.max(sliceStartPos, var.getStart()); // get max start pos (in case of backwards search)
            int end = Math.min(nextSliceStartPos, var.getEnd()); // Don't go over the edge of the slice
            // One entry per position in region
            for(int vcfPos = start; vcfPos <= end; ++vcfPos){
                List<Variant> list = resMap.get(vcfPos);
                if(list == null){
                    list = new ArrayList<Variant>();
                }
                list.add(var);
            }
            minStartPos = Math.min(minStartPos,start); // update overall min
        }
        return minStartPos;
    }

    protected List<Genotype> extractGts(Variant var) {
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
            context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-query").increment(1);
            VariantTableHelper h = getHelper();
            Scan scan = new Scan(sliceKey, Bytes.toBytes(endKey));
            scan.setFilter(new PrefixFilter(sliceKey));
            scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(getHelper().getStudyId() + "_")));  // Limit for currenty study
            ResultScanner rs = table.getScanner(scan); 
            for(Result r : rs){
                String rowStr = Bytes.toString(r.getRow());
                Integer pos = h.extractPositionFromVariantRowKey(rowStr);
                context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-result").increment(1);
                if(!r.isEmpty()){ // only non empty rows
                    Map<String, Result> res = resMap.get(pos);
                    if(null == res){
                        res = new HashMap<String, Result>();
                        resMap.put(pos, res);
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
