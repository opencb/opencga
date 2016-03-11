package org.opencb.opencga.storage.hadoop.variant.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
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

import com.google.common.collect.BiMap;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public abstract class AbstractVariantTableMapReduce extends TableMapper<ImmutableBytesWritable, Put> {
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration = null;
    private Connection dbConnection = null;

    private Map<String, Long> timeSum = new HashMap<>();
    private ArchiveResultToVariantConverter resultConverter;

    private VariantMerger variantMerger;
    private HBaseToVariantConverter hbaseToVariantConverter;
    private SortedMap<Long, String> times = new TreeMap<>();

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

    protected SortedMap<Long, String> getTimes() {
        return times;
    }

    public Map<String, Long> getTimeSum() {
        return timeSum;
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
        try (ArchiveFileMetadataManager metadataManager = new ArchiveFileMetadataManager(tableName, conf, null)) {
            QueryResult<VcfMeta> allVcfMetas = metadataManager.getAllVcfMetas(new ObjectMap());
            for (VcfMeta vcfMeta : allVcfMetas.getResult()) {
                vcfMetaMap.put(Integer.parseInt(vcfMeta.getVariantSource().getFileId()), vcfMeta);
            }
        }
        getLog().info("Loaded {} VcfMETA data!!!", vcfMetaMap.size());
        return vcfMetaMap;
    }

    protected void addTime(String done) {
        getTimes().put(System.currentTimeMillis(), done);
    }

    protected void addTimes() {
        long prev = -1;
        String id = "";
        for (Entry<Long, String> e : getTimes().entrySet()) {
            if (prev > 0) {
                long curr = getTimeSum().getOrDefault(id, 0L);
                long diff = e.getKey() - prev;
                if (prev < 0) {
                    diff = 0;
                }
                getTimeSum().put(id, curr + diff);
            }
            prev = e.getKey();
            id = e.getValue();
        }
        getTimes().clear();
    }

    private Set<Integer> extractFileIds(Result value) {
        Set<String> fieldIds = new HashSet<String>();
        for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> grp : value.getMap().entrySet()) {
            Set<String> keys = grp.getValue().keySet().stream().map(k -> Bytes.toString(k)).collect(Collectors.toSet());
            fieldIds.addAll(keys);
        }
        Set<Integer> fieldInts = fieldIds.stream().filter(s -> !StringUtils.equals(GenomeHelper.VARIANT_COLUMN, s))
                .map(s -> Integer.parseInt(s)).collect(Collectors.toSet());
        return fieldInts;
    }

    private List<Variant> parseCurrentVariantsRegion(Result value, String chr)
            throws InvalidProtocolBufferException {
        List<VariantTableStudyRow> tableStudyRows;
        byte[] protoData = value.getValue(getHelper().getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
        if (protoData != null && protoData.length > 0) {
            VariantTableStudyRowsProto variantTableStudyRowsProto = VariantTableStudyRowsProto.parseFrom(protoData);
            tableStudyRows = new ArrayList<>(variantTableStudyRowsProto.getRowsCount());
            for (VariantTableStudyRowProto variantTableStudyRowProto : variantTableStudyRowsProto.getRowsList()) {
                tableStudyRows.add(new VariantTableStudyRow(variantTableStudyRowProto, chr, getStudyConfiguration().getStudyId()));
            }

            List<Variant> variants = new ArrayList<>(tableStudyRows.size());
            for (VariantTableStudyRow tableStudyRow : tableStudyRows) {
                variants.add(getHbaseToVariantConverter().convert(tableStudyRow));
            }
            return variants;

        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Load (if available) current data, merge information and store new object in DB.
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
            Put put = null;
            if (null != newSampleIds) {
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

    protected void updateArchiveTable(ImmutableBytesWritable key, Context context, List<VariantTableStudyRow> tableStudyRows)
            throws IOException, InterruptedException {
        Put put = new Put(key.get());
        put.addColumn(getHelper().getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B,
                VariantTableStudyRow.toProto(tableStudyRows).toByteArray());
        context.write(new ImmutableBytesWritable(getHelper().getIntputTable()), put);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW_PUT").increment(1);
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROWS_IN_PUT").increment(tableStudyRows.size());
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
        getTimes().clear();
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

            Set<Integer> fileIds = extractFileIds(value);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Results contain file IDs : " + StringUtils.join(fileIds, ','));
            }
            Set<Integer> sampleIds = new HashSet<>();
            for (Integer fid : fileIds) {
                LinkedHashSet<Integer> sids = getStudyConfiguration().getSamplesInFiles().get(fid);
                sampleIds.addAll(sids);
            }

            getLog().info("Processing slice {}", sliceKey);

            addTime("Load Analysis");

            // Analysis: Load variants for region (study specific)
//            List<Variant> analysisVar = loadCurrentVariantsRegion(context, chr, startPos, nextStartPos);
            List<Variant> analysisVar = parseCurrentVariantsRegion(value, chr);
            context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
            getLog().info("Loaded {} variants ... ", analysisVar.size());
            if (!analysisVar.isEmpty()) {
                Variant tmpVar = analysisVar.get(0);
                if (getLog().isDebugEnabled()) {
                    getLog().debug("Loaded variant from analysis table: " + tmpVar.toJson());
                }
            }
            VariantMapReduceContect ctx = new VariantMapReduceContect();
            ctx.context = context;
            ctx.currRowKey = currRowKey;
            ctx.key = key;
            ctx.value = value;
            ctx.sliceKey = sliceKey;
            ctx.fileIds = fileIds;
            ctx.sampleIds = sampleIds;
            ctx.startPos = startPos;
            ctx.nextStartPos = nextStartPos;
            ctx.analysisVar = analysisVar;

            /* *********************************** */
            /* ********* CALL concrete class ***** */
            doMap(ctx);
            /* *********************************** */
            addTime("Done");
            addTimes();
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    abstract void doMap(VariantMapReduceContect ctx) throws IOException, InterruptedException;

    protected class VariantMapReduceContect {
        public byte[] currRowKey;
        protected Context context;
        protected ImmutableBytesWritable key;
        protected Result value;
        protected String sliceKey;
        protected Set<Integer> fileIds;
        protected Set<Integer> sampleIds;
        protected long startPos;
        protected long nextStartPos;
        protected List<Variant> analysisVar;
    }
}
