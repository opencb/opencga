/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveFileMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowProto;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public static final VariantType[] TARGET_VARIANT_TYPE = new VariantType[] { VariantType.SNV, VariantType.SNP };
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";

    private final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration = null;
//    private final Map<Integer, VcfMeta> vcfMetaMap = new ConcurrentHashMap<>();
    private Connection dbConnection = null;
    private Map<String, Long> timeSum = new HashMap<>();
    private ArchiveResultToVariantConverter resultConverter;
    private VariantMerger variantMerger;
    private HBaseToVariantConverter hbaseToVariantConverter;
    private SortedMap<Long, String> times = new TreeMap<>();

    private Logger getLog() {
        return LOG;
    }


    /*
     *
     *             +---------+----------+
     *             | ARCHIVE | ANALYSIS |
     *  +----------+---------+----------+
     *  | 1:10:A:T |   DATA  |   ----   |   <= New variant
     *  +----------+---------+----------+
     *  | 1:20:C:G |   ----  |   DATA   |   <= Missing variant
     *  +----------+---------+----------+
     *  | 1:30:G:T |   DATA  |   DATA   |   <= Same variant
     *  +----------+---------+----------+
     *  | 1:40:T:C |   DATA  |   ----   |   <= Overlapped variant (new)
     *  | 1:40:T:G |   ----  |   DATA   |   <= Overlapped variant (missing)
     *  +----------+---------+----------+
     *
     */
    public enum OpenCGAVariantTableCounters {
        ARCHIVE_TABLE_VARIANTS,
        ANALYSIS_TABLE_VARIANTS,
        NEW_VARIANTS,
        MISSING_VARIANTS,
        SAME_VARIANTS
    }


    @Override
    protected void setup(Context context) throws IOException,
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
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Entry<String, Long> entry : this.timeSum.entrySet()) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_TIMER_" + entry.getKey().replace(' ', '_')).increment(entry.getValue());
        }
        if (null != this.dbConnection) {
            dbConnection.close();
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        times.clear();
        if (value.isEmpty()) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }
        try {
            if (Bytes.equals(key.get(), getHelper().getMetaRowKey())) {
                return; // ignore metadata column
            }

            context.getCounter(COUNTER_GROUP_NAME, "VCF_BLOCK_READ").increment(1);

            addTime("Unpack slice");

            // Calculate various positions
            byte[] currRowKey = key.get();
            String sliceKey = Bytes.toString(currRowKey);
            VariantTableHelper h = getHelper();
            String chr = h.extractChromosomeFromBlockId(sliceKey);
            Long sliceReg = h.extractSliceFromBlockId(sliceKey);
            long startPos = h.getStartPositionFromSlice(sliceReg);
            long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);

            Set<String> fileIds = extractFileIds(value);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Results contain file IDs : " + StringUtils.join(fileIds, ','));
            }
            Set<Integer> newSampleIds = new HashSet<>();
            for (String fid : fileIds) {
                if (!StringUtils.equals(GenomeHelper.VARIANT_COLUMN, fid)) {
                    LinkedHashSet<Integer> sids = getStudyConfiguration().getSamplesInFiles().get(Integer.parseInt(fid));
                    newSampleIds.addAll(sids);
                }
            }

            getLog().info("Processing slice {}", sliceKey);

            // Archive: unpack Archive data (selection only
            List<Variant> archiveVar = getResultConverter().convert(value, startPos, nextStartPos, true);
            context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE").increment(archiveVar.size());
            addTime("Filter slice");
            // Variants of target type
            List<Variant> archiveTarget = filterForVariant(archiveVar.stream(), TARGET_VARIANT_TYPE).collect(Collectors.toList());
            if (!archiveTarget.isEmpty()) {
                Variant tmpVar = archiveTarget.get(0);
                if (getLog().isDebugEnabled()) {
                    getLog().debug("Loaded variant from archive table: " + tmpVar.toJson());
                }
            }
            context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE_TARGET").increment(archiveTarget.size());
            addTime("Load Analysis");

            // Analysis: Load variants for region (study specific)
//            List<Variant> analysisVar = loadCurrentVariantsRegion(context, chr, startPos, nextStartPos);
            List<Variant> analysisVar = parseCurrentVariantsRegion(value, chr);
            context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());

//            Set<Variant> analysisVarSet = new HashSet<>(analysisVar);
            Set<String> analysisVarSet = analysisVar.stream().map(Variant::toString).collect(Collectors.toSet());
            getLog().info("Loaded {} variants ... ", analysisVar.size());
            if (!analysisVar.isEmpty()) {
                Variant tmpVar = analysisVar.get(0);
                if (getLog().isDebugEnabled()) {
                    getLog().debug("Loaded variant from analysis table: " + tmpVar.toJson());
                }
            }
            addTime("Check consistency");

            // Check if Archive covers all bases in Analysis
            checkArchiveConsistency(context, startPos, nextStartPos, archiveVar, analysisVar);

            /* ******** Update Analysis Variants ************** */
            // (1) NEW variants (only create the position, no filling yet)
            addTime("Merge NEW variants");
            Set<Variant> analysisNew = new HashSet<>();
            Set<String> archiveTargetSet = new HashSet<>();
            for (Variant tar : archiveTarget) {
                // Get all the archive target variants that are not in the analysis variants.
//                Optional<Variant> any = analysisVar.stream().filter(v -> VariantMerger.isSameVariant(v, tar)).findAny();
                // is new Variant?
                String tarString = tar.toString();
                archiveTargetSet.add(tarString);
                if (!analysisVarSet.contains(tarString)) {
                    // Empty variant with no Sample information
                    // Filled with Sample information later (see 2)
                    Variant tarNew = this.variantMerger.createFromTemplate(tar);
                    analysisNew.add(tarNew);
                }
            }

            int sameVariants = archiveTargetSet.size() - analysisNew.size();
            context.getCounter(OpenCGAVariantTableCounters.NEW_VARIANTS).increment(analysisNew.size());
            context.getCounter(OpenCGAVariantTableCounters.SAME_VARIANTS).increment(sameVariants);
            context.getCounter(OpenCGAVariantTableCounters.MISSING_VARIANTS).increment(analysisVar.size() - sameVariants);
            context.getCounter(OpenCGAVariantTableCounters.ARCHIVE_TABLE_VARIANTS).increment(archiveTargetSet.size());
            context.getCounter(OpenCGAVariantTableCounters.ANALYSIS_TABLE_VARIANTS).increment(analysisVar.size());

            // with current files of same region
            for (Variant var : analysisNew) {
                this.variantMerger.merge(var, archiveVar);
            }
            // with all other gVCF files of same region
            if (!analysisNew.isEmpty()) {
                addTime("Load archive slice from hbase");
                List<Variant> archiveOther = loadFromArchive(context, sliceKey, fileIds);
                if (!archiveOther.isEmpty()) {
                    context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE").increment(archiveOther.size());
                    context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_NUM_QUERIES").increment(1);
                    addTime("Merge NEW with archive slice");
                    for (Variant var : analysisNew) {
                        this.variantMerger.merge(var, archiveOther);
                    }
                }
            }

            // (2) and (3): Same / overlapping position
            addTime("Merge same / overlapping");
            for (Variant var : analysisVar) {
                this.variantMerger.merge(var, archiveVar);
            }
            // (1) Merge NEW into Analysis table
//            analysisVar.addAll(analysisNew);

            addTime("Store analysis");

            // fetchCurrentValues(context, summary.keySet());
            List<VariantTableStudyRow> rows = new ArrayList<>(analysisNew.size() + analysisVar.size());
            updateOutputTable(context, analysisNew, rows, null);
            updateOutputTable(context, analysisVar, rows, newSampleIds);

            updateArchiveTable(key, context, rows);

            addTime("Done");
            addTimes();
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    private List<Variant> parseCurrentVariantsRegion(Result value, String chr)
            throws InvalidProtocolBufferException {

        List<VariantTableStudyRow> tableStudyRows;
        byte[] protoData = value.getValue(helper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
        if (protoData != null && protoData.length > 0) {
            VariantTableStudyRowsProto variantTableStudyRowsProto = VariantTableStudyRowsProto.parseFrom(protoData);
            tableStudyRows = new ArrayList<>(variantTableStudyRowsProto.getRowsCount());
            for (VariantTableStudyRowProto variantTableStudyRowProto : variantTableStudyRowsProto.getRowsList()) {
                tableStudyRows.add(new VariantTableStudyRow(variantTableStudyRowProto, chr, studyConfiguration.getStudyId()));
            }

            List<Variant> variants = new ArrayList<>(tableStudyRows.size());
            for (VariantTableStudyRow tableStudyRow : tableStudyRows) {
                variants.add(hbaseToVariantConverter.convert(tableStudyRow));
            }
            return variants;

        } else {
            return Collections.emptyList();
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
        if (getLog().isDebugEnabled()) {
            getLog().debug("Add files to search in archive: " + StringUtils.join(archiveFileIds, ','));
        }
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
            context.getCounter(COUNTER_GROUP_NAME, "VCF_VARIANT-error-FIXME").increment(1);
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

    private void addTime(String done) {
        times.put(System.currentTimeMillis(), done);
    }

    private void addTimes() {
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
        times.clear();
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
     * @param context {@link Context}
     * @param chr Chromosome
     * @param start Start (inclusive) position
     * @param end End (exclusive) position
     * @return L
     * @throws IOException If any IO issue occurs
     * @deprecated Do not read from VariantTable anymore! Use {@link #parseCurrentVariantsRegion} instead
     */
    @Deprecated
    protected List<Variant> loadCurrentVariantsRegion(Context context, String chr, Long start, Long end)
            throws IOException {
        String colPrefix = getHelper().getStudyId() + "_";
        byte[] startKey = getHelper().generateVariantPositionPrefix(chr, start);
        byte[] endKey = getHelper().generateVariantPositionPrefix(chr, end);
        List<Variant> analysisVariants = new ArrayList<Variant>();
//        boolean foundScan = false; // FIXME clean up
        try (Table table = getDbConnection().getTable(TableName.valueOf(getHelper().getOutputTable()));) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_TABLE_SCAN-query").increment(1);
            if (getLog().isDebugEnabled()) {
                getLog().debug(String.format("Scan chr %s from %s to %s with column prefix %s", chr, start, end, colPrefix));
            }

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
     * @param rows
     * @param newSampleIds
     * @throws IOException
     * @throws InterruptedException
     */
    private List<VariantTableStudyRow> updateOutputTable(Context context, Collection<Variant> analysisVar, List<VariantTableStudyRow> rows,
            Set<Integer> newSampleIds) throws IOException, InterruptedException {
        int studyId = getStudyConfiguration().getStudyId();
        BiMap<String, Integer> idMapping = getStudyConfiguration().getSampleIds();
        for (Variant variant : analysisVar) {
            VariantTableStudyRow row = new VariantTableStudyRow(variant, studyId, idMapping);
            rows.add(row);
            Put put = null;
            if (null != newSampleIds) {
                put = row.createSpecificPut(getHelper(), newSampleIds);
            } else {
                put = row.createPut(getHelper());
            }
            if (put != null) {
                context.write(new ImmutableBytesWritable(helper.getOutputTable()), put);
                context.getCounter(COUNTER_GROUP_NAME, "VARIANT_TABLE_ROW-put").increment(1);
            }
        }
        return rows;
    }

    private void updateArchiveTable(ImmutableBytesWritable key, Context context, List<VariantTableStudyRow> tableStudyRows)
            throws IOException, InterruptedException {
        Put put = new Put(key.get());
        put.addColumn(helper.getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B,
                VariantTableStudyRow.toProto(tableStudyRows).toByteArray());
        context.write(new ImmutableBytesWritable(helper.getIntputTable()), put);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW_PUT").increment(1);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROWS_IN_PUT").increment(tableStudyRows.size());
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
        getLog().info("Loaded {} VcfMETA data!!!", vcfMetaMap.size());
        return vcfMetaMap;
    }

    private ArchiveResultToVariantConverter getResultConverter() {
        return resultConverter;
    }

    private StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

}
