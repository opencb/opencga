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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static final VariantType[] TARGET_VARIANT_TYPE = new VariantType[] { VariantType.SNV, VariantType.SNP };

    private final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration = null;
//    private final Map<Integer, VcfMeta> vcfMetaMap = new ConcurrentHashMap<>();
    private Connection dbConnection = null;
    private Map<String, Long> timeSum = new HashMap<String, Long>();
    private ArchiveResultToVariantConverter resultConverter;
    private VariantMerger variantMerger;
    private HBaseToVariantConverter hbaseToVariantConverter;

    private Logger getLog() {
        return LOG;
    }

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        getLog().debug("Setup configuration");
        // Setup configuration
        helper = new VariantTableHelper(context.getConfiguration());
        this.studyConfiguration = getHelper().loadMeta(); // Variant meta

        // Open DB connection
        dbConnection = ConnectionFactory.createConnection(context.getConfiguration());

        // Load VCF meta data for columns
        Map<Integer, VcfMeta> vcfMetaMap = loadVcfMetaMap(context.getConfiguration()); // Archive meta
        resultConverter = new ArchiveResultToVariantConverter(vcfMetaMap, helper.getColumnFamily());
        hbaseToVariantConverter = new HBaseToVariantConverter(this.helper);
        variantMerger = new VariantMerger();
        super.setup(context);
    }

    @Override
    protected void cleanup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        for (Entry<String, Long> entry : this.timeSum.entrySet()) {
            context.getCounter("OPENCGA.HBASE", "VCF_TIMER_" + entry.getKey()).increment(entry.getValue());
        }
        if (null != this.dbConnection) {
            dbConnection.close();
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        if (value.isEmpty()) {
            context.getCounter("OPENCGA.HBASE", "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }
        try {
            if (Bytes.equals(key.get(), getHelper().getMetaRowKey())) {
                return; // ignore metadata column
            }

            context.getCounter("OPENCGA.HBASE", "VCF_BLOCK_READ").increment(1);
            SortedMap<Long, String> times = new TreeMap<Long, String>();

            times.put(System.currentTimeMillis(), "Unpack slice");

            // Calculate various positions
            byte[] currRowKey = key.get();
            String sliceKey = Bytes.toString(currRowKey);
            VariantTableHelper h = getHelper();
            String chr = h.extractChromosomeFromBlockId(sliceKey);
            Long sliceReg = h.extractSliceFromBlockId(sliceKey);
            long startPos = h.getStartPositionFromSlice(sliceReg);
            long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);

            Set<String> fileIds = extractFileIds(value);
            getLog().info("Results contain file IDs : " + StringUtils.join(fileIds, ','));

            getLog().info("Processing slice " + sliceKey);

            // Archive: unpack Archive data (selection only
            List<Variant> archiveVar = getResultConverter().convert(value, startPos,  nextStartPos, true);
            times.put(System.currentTimeMillis(), "Filter slice");
            // Variants of target type
            List<Variant> archiveTarget = filterForVariant(archiveVar.stream(), TARGET_VARIANT_TYPE).collect(Collectors.toList());
            if (!archiveTarget.isEmpty()) {
                Variant tmpVar = archiveTarget.get(0);
                getLog().info("Loaded variant from archive table: " + tmpVar.toJson());
            }
            times.put(System.currentTimeMillis(), "Load Analysis");

            // Analysis: Load variants for region (study specific)
            List<Variant> analysisVar = loadCurrentVariantsRegion(context, chr, startPos, nextStartPos);
            getLog().info(String.format("Loaded %s variants ... ", analysisVar.size()));
            if (!analysisVar.isEmpty()) {
                Variant tmpVar = analysisVar.get(0);
                getLog().info("Loaded variant from analysi table: " + tmpVar.toJson());
            }
            times.put(System.currentTimeMillis(), "Check consistency");

            // Check if Archive covers all bases in Analysis
            checkArchiveConsistency(context, startPos, nextStartPos, archiveVar, analysisVar);

            /* ******** Update Analysis Variants ************** */
            // (1) NEW variants (only create the position, no filling yet)
            times.put(System.currentTimeMillis(), "Merge NEW variants");
            List<Variant> analysisNew = new ArrayList<Variant>();
            for (Variant tar : archiveTarget) {
                Optional<Variant> any = analysisVar.stream().filter(v -> VariantMerger.isSameVariant(v, tar)).findAny();
                // is new Variant?
                if (!any.isPresent()) {
                    // Empty variant with no Sample information
                    // Filled with Sample information later (see 2)
                    Variant tarNew = this.variantMerger.createFromTemplate(tar);
                    analysisNew.add(tarNew);
                }
            }
            // with current files of same region
            for (Variant var : analysisNew) {
                this.variantMerger.merge(var, archiveVar);
            }
            times.put(System.currentTimeMillis(), "Load archive slice from hbase");
            // with all other gVCF files of same region
            // NOT the most efficient way to do this
            List<Variant> archiveOther = loadFromArchive(context, sliceKey, fileIds);
            times.put(System.currentTimeMillis(), "Merge NEW with archive slice");
            for (Variant var : analysisNew) {
                this.variantMerger.merge(var, archiveOther);
            }
            // (2) and (3): Same / overlapping position
            times.put(System.currentTimeMillis(), "Merge same / overlapping");
            for (Variant var : analysisVar) {
                this.variantMerger.merge(var, archiveVar);
            }
            // (1) Merge NEW into Analysis table
//            analysisVar.addAll(analysisNew);

            times.put(System.currentTimeMillis(), "Store analysis");

            // fetchCurrentValues(context, summary.keySet());
            updateOutputTable(context, analysisNew);
            updateOutputTable(context, analysisVar);

            times.put(System.currentTimeMillis(), "Done");
            addTimes(times);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    /**
     * Load all variants for all files (except in currFileIds) listed in the study configuration for the specified sliceKey.
     * @param context Context
     * @param sliceKey Slice to extract data for
     * @param currFileIds File ids to ignore
     * @return Variants all variants for the slice
     * @throws IOException
     */
    private List<Variant> loadFromArchive(Context context, String sliceKey, Set<String> currFileIds) throws IOException {
        // Extract File IDs to search through
        LinkedHashSet<Integer> indexedFiles = getStudyConfiguration().getIndexedFiles();
        Set<String> archiveFileIds = indexedFiles.stream().map(v -> v.toString()).filter(k -> !currFileIds.contains(k))
                .collect(Collectors.toSet());
        if (archiveFileIds.isEmpty()) {
            getLog().info("No files found to search for in archive table");
            return Collections.emptyList();
        }
        getLog().info("Add files to search in archive: " + StringUtils.join(archiveFileIds, ','));
        Get get = new Get(Bytes.toBytes(sliceKey));
        byte[] cf = getHelper().getColumnFamily();
        archiveFileIds.forEach(e -> get.addColumn(cf, Bytes.toBytes(e)));
        Result res = getHelper().getHBaseManager().act(getDbConnection(), getHelper().getIntputTable(), table -> {
            return table.get(get);
        });
        if (res.isEmpty()) {
            getLog().warn("No data found in archive table!!!");
            return Collections.emptyList();
        }
        List<Variant> var = getResultConverter().convert(res, true);
        return var;
    }

    /**
     * Check if Archive has Variant objects covering all bases (including no-call objects).
     * Increases HBase counter with the name VCF_VARIANT-error-FIXME to act on.
     * @param context
     * @param startPos
     * @param nextStartPos
     * @param archiveVar
     * @param analysisVar
     */
    private void checkArchiveConsistency(Context context, long startPos,
            long nextStartPos, List<Variant> archiveVar, List<Variant> analysisVar) {
        // Report Missing regions in ARCHIVE table, which are seen in VAR table
        Set<Integer> archPosMissing = generateCoveredPositions(analysisVar.stream(), startPos, nextStartPos);
        archPosMissing.removeAll(generateCoveredPositions(archiveVar.stream(), startPos, nextStartPos));
        if (!archPosMissing.isEmpty()) {
            // should never happen - positions exist in variant table but not in archive table
            context.getCounter("OPENCGA.HBASE", "VCF_VARIANT-error-FIXME").increment(1);
            getLog().error(
                    String.format("Positions found in variant table but not in Archive table: %s",
                            Arrays.toString(archPosMissing.toArray(new Integer[0]))));
        }
    }

    private Set<String> extractFileIds(Result value) {
        Set<String> fieldIds = new HashSet<String>();
        for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> grp : value.getMap().entrySet()) {
            Set<String> keys = grp.getValue().keySet().stream().map(k -> Bytes.toString(k)).collect(Collectors.toSet());
            fieldIds.addAll(keys);
        }
        return fieldIds;
    }

    private void addTimes(SortedMap<Long, String> times) {
        long prev = -1;
        String id = "";
        for (Entry<Long, String> e : times.entrySet()) {
            if (prev > 0) {
                long curr = timeSum.getOrDefault(id, 0L);
                long diff = e.getKey() - prev;
                if (prev < 0) {
                    diff = 0;
                }
                timeSum.put(id, curr + diff);
            }
            prev = e.getKey();
            id = e.getValue();
        }
    }

    protected Set<Integer> generateCoveredPositions(Stream<Variant> variants, long startPos, long nextStartPos) {
        final int sPos = (int) startPos;
        final int ePos = (int) (nextStartPos - 1);
        // limit to max start position end min end position (only slice region)
        // hope this works
        return variants.map(v -> generateRegion(Math.max(v.getStart(), sPos), Math.min(v.getEnd(), ePos))).flatMap(l -> l.stream())
                .collect(Collectors.toSet());
    }

    private Set<Integer> generateRegion(Integer start, Integer end) {
        if (end < start) {
            throw new IllegalStateException(String.format("End position (%s) is < than Start (%s)!!!", start, end));
        }
        int len = end - start;
        Integer[] array = new Integer[len + 1];
        for (int a = 0; a <= len; a++) { // <= to be inclusive
            array[a] = (start + a);
        }
        return new HashSet<Integer>(Arrays.asList(array));
    }


    protected Stream<Variant> filterForVariant(Stream<Variant> variants, VariantType ... types) {
        Set<VariantType> whileList = new HashSet<>(Arrays.asList(types));
        return variants.filter(v -> whileList.contains(v.getType()));
    }

    /**
     * Fetch already loaded variants in the Variant Table.
     * @param context {@link org.apache.hadoop.mapreduce.Mapper.Context}
     * @param chr Chromosome
     * @param start Start (inclusive) position
     * @param end End (exclusive) position
     * @return L
     * @throws IOException If any IO issue occurs
     */
    protected List<Variant> loadCurrentVariantsRegion(Context context, String chr, Long start, Long end)
            throws IOException {
        String colPrefix = getHelper().getStudyId() + "_";
        byte[] startKey = getHelper().generateVariantPositionPrefix(chr, start);
        byte[] endKey = getHelper().generateVariantPositionPrefix(chr, end);
        List<Variant> analysisVariants = new ArrayList<Variant>();
//        boolean foundScan = false; // FIXME clean up
        try (Table table = getDbConnection().getTable(TableName.valueOf(getHelper().getOutputTable()));) {
            context.getCounter("OPENCGA.HBASE", "VCF_TABLE_SCAN-query").increment(1);
            getLog().info(
                    String.format("Scan chr %s from %s to %s with column prefix %s", chr, start, end, colPrefix));

            Scan scan = new Scan(startKey, endKey);
            scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(colPrefix))); // Limit to current study
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
//                foundScan = true;
                Variant var = this.hbaseToVariantConverter.convert(r);
                if (var.getStudiesMap().isEmpty()) {
                    throw new IllegalStateException("No Studies registered for variant!!! " + var);
                }
                analysisVariants.add(var);
                if (!r.containsColumn(this.getHelper().getColumnFamily(), Bytes.toBytes(colPrefix + "0/0"))) {
                    throw new IllegalStateException("Hom-ref column not found for prefix: " + var);
                }
            }
        }
//        if (!foundScan) {
//            throw new IllegalStateException(String.format("No result returned after scan using prefix %s", colPrefix));
//        }
//        if (analysisVariants.isEmpty()) {
//            throw new IllegalStateException(String.format("No Variants found using prefix %s", colPrefix));
//        }
//        Set<String> maplst = analysisVariants.stream().flatMap(v -> v.getStudiesMap().keySet().stream()).collect(Collectors.toSet());
//        if (maplst.isEmpty()) {
//            throw new IllegalStateException("No study data loaded at all for " + colPrefix + "; ");
//        }
//        List<Variant> noStudy = analysisVariants.stream().filter(v -> v.getStudy(Integer.toString(getHelper().getStudyId())) == null)
//                .collect(Collectors.toList());
//        if (!noStudy.isEmpty()) {
//            throw new IllegalStateException("Loaded variants with no Study id!!! using prefix  " + colPrefix + "; " + noStudy.size() + ";"
//                    + Strings.join(maplst, ","));
//        }
        return analysisVariants;
    }

    /**
     * Load (if available) current data, merge information and store new object
     * in DB.
     *
     * @param context
     * @param analysisVar
     * @throws IOException
     * @throws InterruptedException
     */
    private void updateOutputTable(Context context, List<Variant> analysisVar)
            throws IOException, InterruptedException {
        int studyId = getStudyConfiguration().getStudyId();
        BiMap<String, Integer> idMapping = getStudyConfiguration().getSampleIds();
        for (Variant variant : analysisVar) {
            VariantTableStudyRow row = VariantTableStudyRow.build(variant, studyId, idMapping);
            Put put = row.createPut(getHelper());
            context.write(new ImmutableBytesWritable(put.getRow()), put);
            context.getCounter("OPENCGA.HBASE", "VCF_ROW-put").increment(1);
        }
    }

    public VariantTableHelper getHelper() {
        return helper;
    }

    protected void setHelper(VariantTableHelper helper) {
        this.helper = helper;
    }

    protected Connection getDbConnection() {
        return dbConnection;
    }

    /**
     * Load VCF Meta data from input table and create table index.
     *
     * @param conf
     *            Hadoop configuration object
     * @throws IOException
     *             If any IO problem occurs
     * @return {@link Map} from file id to {@link VcfMeta}
     */
    protected Map<Integer, VcfMeta> loadVcfMetaMap(Configuration conf) throws IOException {
        Map<Integer, VcfMeta> vcfMetaMap = new HashMap<Integer, VcfMeta>();
        String tableName = Bytes.toString(getHelper().getIntputTable());
        getLog().debug("Load VcfMETA from {}", tableName);
        try (ArchiveFileMetadataManager metadataManager = new ArchiveFileMetadataManager(tableName, conf, null)) {
            QueryResult<VcfMeta> allVcfMetas = metadataManager.getAllVcfMetas(new ObjectMap());
            for (VcfMeta vcfMeta : allVcfMetas.getResult()) {
                vcfMetaMap.put(Integer.parseInt(vcfMeta.getVariantSource().getFileId()), vcfMeta);
            }
        }
        getLog().info(String.format("Loaded %s VcfMETA data!!!", vcfMetaMap.size()));
        return vcfMetaMap;
    }

    private ArchiveResultToVariantConverter getResultConverter() {
        return resultConverter;
    }

    private StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

}
