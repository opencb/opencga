/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
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

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableMapper extends TableMapper<ImmutableBytesWritable, Put>{
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
            int startPos = (int) h.getStartPositionFromSlice(sliceReg);
            int nextStartPos = (int) h.getStartPositionFromSlice(sliceReg + 1);
            byte[] varStartKey = h.generateVariantRowKey(chr, startPos);
            byte[] varEndKey = h.generateVariantRowKey(chr, nextStartPos);

            getLog().info("Processing slice " + sliceKey);
// Unpack Archive data (selection only
            List<Variant> archive = unpack(value, context, startPos, nextStartPos);

            times.add(System.currentTimeMillis());

            Set<Integer> coveredPositions = generateCoveredPositions(archive.stream(), startPos, nextStartPos);
            
            getLog().info(String.format("Found %s covered regions from %s variants",coveredPositions.size(),archive.size()));
            getLog().trace(Arrays.toString(coveredPositions.toArray()));

            // Start positions of Variants of specific type
            Set<Integer> archVarStartPositions = 
                    filterForVariant(archive.stream(), TARGET_VARIANT_TYPE)
                        .map(v -> v.getStart()).collect(Collectors.toSet());

            times.add(System.currentTimeMillis());

// Load Variant data (For study) for same region
            Map<Integer, Map<ByteArray, Result>> varTabMap = loadCurrentVariantsRegion(context, varStartKey, varEndKey);
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

            getLog().info(String.format("Loaded %s variant positions ... ",varTabMap.size()));
            getLog().trace(Arrays.toString(varTabMap.keySet().toArray()));

            // Find ARCHIVE entries covering all existing positions in the VARIANT table
            List<Variant> varPosUpdateLst = filterByPositionCovered(archive, varTabMap.keySet());
            
            getLog().info(String.format("Found %s variants to update ... ",varPosUpdateLst.size()));

            times.add(System.currentTimeMillis());

// Update VARIANT table entries with archive table entries
            Map<ByteArray, VariantTableStudyRow> updatedVar = merge(context, varPosUpdateLst, translate(varTabMap));

            getLog().info(String.format("Merged into %s variants ... ",updatedVar.size()));

            times.add(System.currentTimeMillis());

            // Missing positions in VAR table -> require Archive table fetch of all columns
            Set<Integer> varPosMissing = new HashSet<Integer>(archVarStartPositions);
            varPosMissing.removeAll(varTabMap.keySet());
            // Current 
            List<Variant> archMissingInVar = filterByPositionCovered(archive,varPosMissing);

            times.add(System.currentTimeMillis());

            getLog().info(String.format("Load  %s missing positions!",varPosMissing.size()));
            getLog().trace(Arrays.toString(varPosMissing.toArray()));

            getLog().info("Fetch data for missing position from Archive table: " + varPosMissing.size());
            List<Variant> archPreviousFiles = loadArchivePreviousFiles(context, varPosMissing,sliceKey);
            getLog().info("Fetched Variants from Archive table: " + archPreviousFiles.size());

            times.add(System.currentTimeMillis());

            List<Variant> newTargetVariants = new ArrayList<Variant>(archPreviousFiles);
            newTargetVariants.addAll(archMissingInVar);

            // check that there is no duplication of rows (current mapreduce and reloaded Arch data)
            if(new HashSet<Variant>(newTargetVariants).size() != newTargetVariants.size()){
                throw new IllegalStateException("Double loading of Archive data!!!");
            }

            Map<ByteArray, VariantTableStudyRow> newVar = createNewVar(context, newTargetVariants, varPosMissing);

            getLog().info(String.format("Created %s Variants from %s targets from %s missing positions ... ",
                    newVar.size(),newTargetVariants.size(), varPosMissing.size()));

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
            Integer value = file.getValue();
            getLog().info("Add File ID " + value + " to search ... ");
            get.addColumn(getHelper().getColumnFamily(), Bytes.toBytes(value));
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

    protected Set<Integer> generateCoveredPositions(Stream<Variant> variants, int startPos, int nextStartPos) {
        final int sPos = startPos;
        final int ePos = nextStartPos - 1;
        // limit to max start position end min end position (only slice region)
        return variants.map(v -> generateRegion(Math.max(v.getStart(),sPos),Math.min(v.getEnd(),ePos)))
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

    protected Map<ByteArray, VariantTableStudyRow> createNewVar(MapContext context, List<Variant> archMissing, Set<Integer> targetPos) {
        Map<ByteArray, VariantTableStudyRow> newVar = new HashMap<>();
        
        Map<Variant,Map<String, List<Integer>>> gtIdx = archMissing.stream().collect(Collectors.toMap(v -> v, v -> createGenotypeIndex(v)));
        Map<Variant, ByteArray> rowkeyIdx = archMissing.stream().collect(Collectors.toMap(v -> v, v -> new ByteArray(getHelper().generateVariantRowKey(v))));

        for(Integer pos : targetPos){
            List<Variant> varCovingPos = archMissing.stream().filter(v ->  variantCoveringPosition(v, pos)).collect(Collectors.toList());
            List<Variant> varTargetPos = 
                    filterForVariant(varCovingPos.stream(), TARGET_VARIANT_TYPE)
                    .filter(v -> v.getStart().equals(pos))
                    .collect(Collectors.toList());
            
            for(Variant var : varTargetPos){
                VariantTableStudyRow row = new VariantTableStudyRow(getHelper().getStudyId(), var);
                ByteArray currRowKey = new ByteArray(row.generateRowKey(getHelper()));
                if(newVar.containsKey(currRowKey)){
                    context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new-conflict").increment(1);
                    continue; // already dealt with it
                }
                context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-row-new").increment(1);
                Set<Integer> homRefs = new HashSet<Integer>();
                for(Variant other : varCovingPos){ // also includes the current target  variant
                    Map<String, List<Integer>> gts = gtIdx.get(other);

                    homRefs.addAll(gts.get(VariantTableStudyRow.HOM_REF));
                    if(other.getStart().equals(pos) && Objects.equals(rowkeyIdx.get(other), currRowKey)) { // same Variant
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

    protected Map<ByteArray, VariantTableStudyRow> merge(MapContext context, List<Variant> varPosUpdateLst,
            Map<Integer, Map<ByteArray, VariantTableStudyRow>> positionMap) throws InvalidProtocolBufferException {

        Map<Variant,Map<String, List<Integer>>> gtIdx = 
                varPosUpdateLst.stream().collect(Collectors.toMap(v -> v, v -> createGenotypeIndex(v)));
        Map<Variant, ByteArray> rowkeyIdx = varPosUpdateLst.stream().collect(Collectors.toMap(v -> v, v -> new ByteArray(getHelper().generateVariantRowKey(v))));
        
        /* For each position */
        for(Entry<Integer, Map<ByteArray, VariantTableStudyRow>> positionEntry : positionMap.entrySet()){
            Integer position = positionEntry.getKey();
            Map<ByteArray, VariantTableStudyRow> rowMap = positionEntry.getValue();
            
            /* Find Variants covering this position */
            List<Variant> inPosition = 
                    varPosUpdateLst.stream().filter(v -> variantCoveringPosition(v,position)).collect(Collectors.toList());

            /* For each Row (ref_alt) combination -> update the count*/
            for(Entry<ByteArray, VariantTableStudyRow> rowEntry : rowMap.entrySet()) {
                Set<Integer> homRefSet = new HashSet<Integer>();
                VariantTableStudyRow row = rowEntry.getValue();
                
                /* For each Variant */
                for(Variant var : inPosition){
                    // using row key, since this is "start" position specific and not based on the current position (e.g. if region)
                    ByteArray rowKey = rowkeyIdx.get(var);
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
        Map<ByteArray, List<Variant>> variantsWithVar = filterForVariant(varPosUpdateLst.stream(), TARGET_VARIANT_TYPE)
                .collect(Collectors.groupingBy(v -> rowkeyIdx.get(v)));
        for(Entry<ByteArray, List<Variant>> varEntry : variantsWithVar.entrySet()){
            ByteArray rowKey = varEntry.getKey();
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

            VariantTableStudyRow row = new VariantTableStudyRow(getHelper().getStudyId(), anyVar);

            List<Variant> value = varEntry.getValue();
            Map<String, List<Integer>> gtToSampleIds = createGenotypeIndex(value.toArray(new Variant[value.size()]));

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
        Map<ByteArray, VariantTableStudyRow> resMap =
                positionMap.values().stream()
                    .map(v -> v.values())
                    .flatMap(v -> v.stream())
                    .collect(Collectors.toMap(v -> new ByteArray(v.generateRowKey(getHelper())), v -> v));
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
                case "G/G": // FIXME fix for unexpected GT data G/G instead of 0/0 
                case "T/T":
                case "A/A":
                case "C/C":
                    gt = VariantTableStudyRow.HOM_REF;
                    break;
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

    private Map<Integer, Map<ByteArray, VariantTableStudyRow>> translate(Map<Integer, Map<ByteArray, Result>> map) {
        // Translate to other object
        Map<Integer, Map<ByteArray, VariantTableStudyRow>> rowMap = new HashMap<>();
        for(Entry<Integer, Map<ByteArray, Result>> resMap : map.entrySet()){
            Map<ByteArray, VariantTableStudyRow> studyRowMap = resMap.getValue().entrySet().stream()
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
     * @param value
     *            {@link Result} object from Archive table
     * @param context
     * @param sliceStartPos
     * @param nextSliceStartPos
     * @param reload
     *            TRUE, if values are reloaded from Archive table on demand to
     *            fill gaps - otherwise FALSE
     * @return
     * @throws IOException
     */
    private List<Variant> unpack(Result value, Context context, int sliceStartPos, int nextSliceStartPos) throws IOException {
        List<Variant> variantList = new ArrayList<Variant>();
        NavigableMap<byte[], byte[]> fm = value.getFamilyMap(getHelper().getColumnFamily());
        for (Entry<byte[], byte[]> x : fm.entrySet()) {
            // for each FILE (column in HBase
            context.getCounter("OPENCGA.HBASE", "VCF_SLICE_READ").increment(1);
            List<Variant> varList = archiveCellToVariants(x.getKey(),x.getValue());
            getLog().info(String.format("For Column %s found %s entries", ArchiveHelper.getFileIdFromColumnName(x.getKey()),varList.size()));
            context.getCounter("OPENCGA.HBASE", "READ_VARIANTS").increment(varList.size());
            variantList.addAll(varList);
        }
        List<Variant> keepList = variantList.stream() // only keep variants in overlapping this region
                .filter(v -> variantCoveringRegion(v, sliceStartPos, nextSliceStartPos,true))
                .collect(Collectors.toList());
        return keepList;
    }

    private List<Variant> archiveCellToVariants(byte[] key, byte[] value) throws InvalidProtocolBufferException {
        int fileId = ArchiveHelper.getFileIdFromColumnName(key);
        //TODO: Reuse this converter
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(this.getVcfMeta(fileId));
        VcfSlice vcfSlice = asSlice(value);
        return converter.convert(vcfSlice);
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

    /**
     * Fetch already loaded variants in the Variant Table
     * @param context   MapReduce Context
     * @param startKey  StartKey from the VariantTable
     * @param endKey    EndKey from the VariantTable
     * @return          Map{Start, Map{RowKey, Variant} }
     * @throws IOException
     */
    protected Map<Integer, Map<ByteArray, Result>> loadCurrentVariantsRegion(Context context, byte[] startKey, byte[] endKey) throws IOException {
        Map<Integer, Map<ByteArray, Result>> resMap = new HashMap<>();
        try (
                Table table = getDbConnection().getTable(TableName.valueOf(getHelper().getOutputTable()));
        ) {
            context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-query").increment(1);
            VariantTableHelper h = getHelper();
            String colPrefix = getHelper().getStudyId() + "_";
            getLog().info(String.format("Scan from %s to %s with column prefix %s", Arrays.toString(startKey), Arrays.toString(endKey), colPrefix));
            
            Scan scan = new Scan(startKey, endKey);
            scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(colPrefix)));  // Limit for currenty study
            ResultScanner rs = table.getScanner(scan); 
            for(Result r : rs){
                byte[] rowStr = r.getRow();
                Variant variant = h.extractVariantFromVariantRowKey(rowStr);
//                Integer pos = h.extractPositionFromVariantRowKey(rowStr);
                context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-result").increment(1);
                if(!r.isEmpty()){ // only non empty rows
                    Map<ByteArray, Result> res = resMap.get(variant.getStart());
                    if(null == res){
                        res = new HashMap<>();
                        resMap.put(variant.getStart(), res);
                    }
                    res.put(new ByteArray(rowStr), r);
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
    private void updateOutputTable(Context context, Map<ByteArray, VariantTableStudyRow> variants)
            throws IOException, InterruptedException {
        for(VariantTableStudyRow row : variants.values()){
            getLog().info(String.format("Submit %s: hr: %s; 0/1: %s",row.getPos(),row.getHomRefCount(), Arrays.toString(row.getSampleIds("0/1").toArray())));
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
