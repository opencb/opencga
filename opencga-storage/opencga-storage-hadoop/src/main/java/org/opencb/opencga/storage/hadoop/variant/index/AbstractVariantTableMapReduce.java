package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Abstract variant table map reduce.
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public abstract class AbstractVariantTableMapReduce extends TableMapper<ImmutableBytesWritable, Mutation> {
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";
    public static final String SPECIFIC_PUT = "opencga.storage.hadoop.hbase.merge.use_specific_put";
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private VariantTableHelper helper;
    protected StudyConfiguration studyConfiguration = null;
    private Connection dbConnection = null;

    protected ArchiveResultToVariantConverter resultConverter;

    protected VariantMerger variantMerger;
    protected HBaseToVariantConverter hbaseToVariantConverter;
    private SortedMap<Long, String> times = new TreeMap<>();
    private long lastTime;
    private Map<String, Long> timeSum = new HashMap<>();

    protected long timestamp = HConstants.LATEST_TIMESTAMP;

    protected Logger getLog() {
        return LOG;
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

    protected ArchiveResultToVariantConverter getResultConverter() {
        return resultConverter;
    }

    protected StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

    protected VariantMerger getVariantMerger() {
        return variantMerger;
    }

    protected HBaseToVariantConverter getHbaseToVariantConverter() {
        return hbaseToVariantConverter;
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
        try (HadoopVariantSourceDBAdaptor metadataManager = new HadoopVariantSourceDBAdaptor(conf)) {
            Iterator<VariantSource> iterator = metadataManager.iterator(studyConfiguration.getStudyId(), null);
            while (iterator.hasNext()) {
                VariantSource variantSource = iterator.next();
                vcfMetaMap.put(Integer.parseInt(variantSource.getFileId()), new VcfMeta(variantSource));
            }
        }
        getLog().info("Loaded {} VcfMETA data!!!", vcfMetaMap.size());
        return vcfMetaMap;
    }

    /**
     * Sets the lastTime value to the {@link System#currentTimeMillis}.
     */
    protected void startTime() {
        lastTime = System.currentTimeMillis();
    }

    /**
     * Calculates the delay between the last saved time and the current {@link System#currentTimeMillis}
     * Resets the last time.
     *
     * @param name Name of the last code block
     */
    protected void endTime(String name) {
        long time = System.currentTimeMillis();
        timeSum.put(name, time - lastTime);
        lastTime = time;
    }

    private Set<Integer> extractFileIds(Result value) {
        NavigableMap<byte[], byte[]> map = value.getNoVersionMap().get(getHelper().getColumnFamily());
        Set<Integer> fileIds = map.keySet().stream().map(k -> Bytes.toString(k))
                .filter(s -> !StringUtils.equals(GenomeHelper.VARIANT_COLUMN, s))
                .map(s -> Integer.parseInt(s)).collect(Collectors.toSet());
        return fileIds;
    }

    protected List<Variant> parseCurrentVariantsRegion(Result value, String chromosome)
            throws InvalidProtocolBufferException {

        List<VariantTableStudyRow> tableStudyRows = parseVariantStudyRowsFromArchive(value, chromosome);

        HBaseToVariantConverter converter = getHbaseToVariantConverter();

        List<Variant> variants = new ArrayList<>(tableStudyRows.size());
        for (VariantTableStudyRow tableStudyRow : tableStudyRows) {
            variants.add(converter.convert(tableStudyRow));
        }
        return variants;

    }

    protected List<VariantTableStudyRow> parseVariantStudyRowsFromArchive(Result value, String chr) throws InvalidProtocolBufferException {
        Cell latestCell = value.getColumnLatestCell(getHelper().getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
        if (null != latestCell) {
            byte[] protoData = CellUtil.cloneValue(latestCell);
            if (protoData != null && protoData.length > 0) {
                VariantTableStudyRowsProto variantTableStudyRowsProto = VariantTableStudyRowsProto.parseFrom(protoData);
                List<VariantTableStudyRow> tableStudyRows = parseVariantStudyRowsFromArchive(chr,
                        variantTableStudyRowsProto);
                return tableStudyRows;
            }
        }
        return Collections.emptyList();
    }

    protected List<VariantTableStudyRow> parseVariantStudyRowsFromArchive(String chr, VariantTableStudyRowsProto
            variantTableStudyRowsProto) {
        return variantTableStudyRowsProto.getRowsList().stream()
                            .map(v -> new VariantTableStudyRow(v, chr, getStudyConfiguration().getStudyId()))
                            .collect(Collectors.toList());
    }

    /**
     * Load (if available) current data, merge information and store new object in DB.
     *
     * @param context Context
     * @param analysisVar Analysis variants
     * @param rows Variant Table rows
     * @param newSampleIds Sample Ids currently processed
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    protected void updateOutputTable(Context context, Collection<Variant> analysisVar,
            List<VariantTableStudyRow> rows, Set<Integer> newSampleIds) throws IOException, InterruptedException {
        int studyId = getStudyConfiguration().getStudyId();
        BiMap<String, Integer> idMapping = getStudyConfiguration().getSampleIds();
        for (Variant variant : analysisVar) {
            VariantTableStudyRow row = new VariantTableStudyRow(variant, studyId, idMapping);
            rows.add(row);
            boolean specificPut = context.getConfiguration().getBoolean(SPECIFIC_PUT, true);
            Put put = null;
            if (specificPut && null != newSampleIds) {
                put = row.createSpecificPut(getHelper(), newSampleIds);
            } else {
                put = row.createPut(getHelper());
            }
            if (put != null) {
                context.write(new ImmutableBytesWritable(getHelper().getOutputTable()), put);
                context.getCounter(COUNTER_GROUP_NAME, "VARIANT_TABLE_ROW-put").increment(1);
            }
        }
    }

    protected void updateOutputTable(Context context, Collection<VariantTableStudyRow> variants) throws IOException, InterruptedException {

        for (VariantTableStudyRow variant : variants) {
            Put put = variant.createPut(getHelper());

            if (put != null) {
                context.write(new ImmutableBytesWritable(getHelper().getOutputTable()), put);
                context.getCounter(COUNTER_GROUP_NAME, "VARIANT_TABLE_ROW-put").increment(1);
            }
        }
    }

    protected void updateArchiveTable(byte[] rowKey, Context context, List<VariantTableStudyRow> tableStudyRows)
            throws IOException, InterruptedException {
        Put put = new Put(rowKey);
        byte[] value = VariantTableStudyRow.toProto(tableStudyRows, timestamp).toByteArray();
        put.addColumn(getHelper().getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B, value);
        context.write(new ImmutableBytesWritable(getHelper().getIntputTable()), put);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW_PUT").increment(1);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROWS_IN_PUT").increment(tableStudyRows.size());
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Thread.currentThread().setName(context.getTaskAttemptID().toString());
        getLog().debug("Setup configuration");

        // Open DB connection
        dbConnection = ConnectionFactory.createConnection(context.getConfiguration());

        // Setup configuration
        helper = new VariantTableHelper(context.getConfiguration(), dbConnection);
        this.studyConfiguration = getHelper().loadMeta(); // Variant meta

        // Load VCF meta data for columns
        Map<Integer, VcfMeta> vcfMetaMap = loadVcfMetaMap(context.getConfiguration()); // Archive meta
        vcfMetaMap.forEach((k, v) -> LOG.info(
                "Loaded Meta Map: File id idx {}; FileId: {} Study {};",
                k, v.getVariantSource().getFileId(), v.getVariantSource().getStudyId()));
        resultConverter = new ArchiveResultToVariantConverter(vcfMetaMap, helper.getColumnFamily());
        hbaseToVariantConverter = new HBaseToVariantConverter(this.helper).setFailOnEmptyVariants(true);
        variantMerger = new VariantMerger();

        timestamp = context.getConfiguration().getLong(AbstractVariantTableDriver.TIMESTAMP, -1);
        if (timestamp == -1) {
            throw new IllegalArgumentException("Missing TimeStamp");
        }

        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        if (null != this.dbConnection) {
            dbConnection.close();
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        getLog().info("Start mapping key: " + Bytes.toString(key.get()));
        startTime();
        if (value.isEmpty()) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }

        if (Bytes.equals(key.get(), getHelper().getMetaRowKey())) {
            return; // ignore metadata column
        }
        context.getCounter(COUNTER_GROUP_NAME, "VCF_BLOCK_READ").increment(1);


        // Calculate various positions
        byte[] currRowKey = key.get();
        String sliceKey = Bytes.toString(currRowKey);
        VariantTableHelper h = getHelper();
        String chr = h.extractChromosomeFromBlockId(sliceKey);
        Long sliceReg = h.extractSliceFromBlockId(sliceKey);
        long startPos = h.getStartPositionFromSlice(sliceReg);
        long nextStartPos = h.getStartPositionFromSlice(sliceReg + 1);

        Set<Integer> fileIds = extractFileIds(value);
        if (getLog().isDebugEnabled()) {
            getLog().debug("Results contain file IDs : " + StringUtils.join(fileIds, ','));
        }
        Set<Integer> sampleIds = new HashSet<>();
        for (Integer fid : fileIds) {
            LinkedHashSet<Integer> sids = getStudyConfiguration().getSamplesInFiles().get(fid);
            sampleIds.addAll(sids);
        }

        getLog().debug("Processing slice {}", sliceKey);


        VariantMapReduceContext ctx = new VariantMapReduceContext(currRowKey, context, value, fileIds,
                sampleIds, chr, startPos, nextStartPos);

        endTime("1 Prepare slice");

        /* *********************************** */
        /* ********* CALL concrete class ***** */
        doMap(ctx);
        /* *********************************** */

        // Clean up of this slice
        for (Entry<String, Long> entry : this.timeSum.entrySet()) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_TIMER_" + entry.getKey().replace(' ', '_')).increment(entry.getValue());
        }
        this.timeSum.clear();
        getLog().info("Finished mapping key: " + Bytes.toString(key.get()));
    }

    abstract void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException;

    protected static class VariantMapReduceContext {
        public VariantMapReduceContext(byte[] currRowKey, Context context, Result value, Set<Integer> fileIds,
                                       Set<Integer> sampleIds, String chr, long startPos, long nextStartPos) {
            this.currRowKey = currRowKey;
            this.context = context;
            this.value = value;
            this.fileIds = fileIds;
            this.sampleIds = sampleIds;
            this.chr = chr;
            this.startPos = startPos;
            this.nextStartPos = nextStartPos;
        }

        protected final byte[] currRowKey;
        protected final Context context;
        protected final Result value;
        protected final Set<Integer> fileIds;
        protected final Set<Integer> sampleIds;
        private final String chr;
        protected final long startPos;
        protected final long nextStartPos;


        public byte[] getCurrRowKey() {
            return currRowKey;
        }

        public Context getContext() {
            return context;
        }

        public Result getValue() {
            return value;
        }

        public Set<Integer> getSampleIds() {
            return sampleIds;
        }

        public String getChromosome() {
            return chr;
        }

    }
}
