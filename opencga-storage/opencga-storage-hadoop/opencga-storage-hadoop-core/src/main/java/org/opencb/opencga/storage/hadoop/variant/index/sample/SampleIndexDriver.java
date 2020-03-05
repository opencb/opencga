package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.BiMap;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.VariantTableSampleIndexOrderMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.NOT_EQUAL;
import static org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE;

/**
 * Created on 15/05/18.
 *
 * <code>
 * export HADOOP_CLASSPATH=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
 * hadoop jar opencga-storage-hadoop-core-X.Y.Z-dev-jar-with-dependencies.jar
 *      org.opencb.opencga.storage.hadoop.variant.adaptors.sampleIndex.SampleIndexDriver
 *      $VARIANTS_TABLE_NAME
 *      output $SAMPLE_INDEX_TABLE
 *      studyId $STUDY_ID
 *      samples $SAMPLE_IDS
 *      ....
 * </code>
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDriver extends AbstractVariantsTableDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleIndexDriver.class);
    public static final String SAMPLES = "samples";
    public static final String OUTPUT = "output";
    public static final String SECONDARY_ONLY = "secondary-only";
//    public static final String MAIN_ONLY = "main-only";
    public static final String PARTIAL_SCAN_SIZE = "partial-scan-size";

    private static final String SAMPLE_ID_TO_FILE_ID_MAP = "SampleIndexDriver.sampleIdToFileIdMap";
    private static final String FIXED_ATTRIBUTES = "SampleIndexDriver.fixedAttributes";
    private int study;
    private int[] samples;
    private String outputTable;
    private boolean allSamples;
    private boolean secondaryOnly;
    private boolean mainOnly;
    private boolean hasGenotype;
    private TreeSet<Integer> sampleIds;
    private Map<Integer, List<Integer>> sampleIdToFileIdMap;
    private String region;
    private double partialScanSize;
    private List<String> fixedAttributes;
    private boolean multiScan = false;

    @Override
    protected String getJobOperationName() {
        return "sample_index";
    }

    @Override
    protected Class<?> getMapperClass() {
        return SampleIndexerMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + SAMPLES, "<samples>*");
        params.put("--" + VariantStorageOptions.STUDY.key(), "<study>");
        params.put("--" + OUTPUT, "<output-table>");
        params.put("--" + SECONDARY_ONLY, "<true|false>");
//        params.put("--" + MAIN_ONLY, "<main-alternate-only>");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        params.put("--" + PARTIAL_SCAN_SIZE, "<samples-per-scan>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        study = getStudyId();
        if (study < 0) {
            BiMap<String, Integer> map = getMetadataManager().getStudies();
            if (map.size() == 1) {
                study = map.values().iterator().next();
                setStudyId(study);
            } else {
                throw new IllegalArgumentException("Select one study from " + map.keySet());
            }
        }
        outputTable = getParam(OUTPUT);
        if (outputTable == null || outputTable.isEmpty()) {
            outputTable = getTableNameGenerator().getSampleIndexTableName(study);
        }

        secondaryOnly = Boolean.valueOf(getParam(SECONDARY_ONLY, "false"));
//        mainOnly = Boolean.valueOf(getParam(MAIN_ONLY, "false"));

        if (secondaryOnly && mainOnly) {
            throw new IllegalArgumentException("Incompatible options " + secondaryOnly + " and " + mainOnly);
        }

        region = getParam(VariantQueryParam.REGION.key());

        // Max number of samples to be processed in each Scan.
        partialScanSize = Integer.valueOf(getParam(PARTIAL_SCAN_SIZE, "1000"));

        String samplesParam = getParam(SAMPLES);
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        if (samplesParam.equals(VariantQueryUtils.ALL)) {
            allSamples = true;
            samples = null;
        } else {
            allSamples = false;
            List<Integer> sampleIds = new LinkedList<>();
            for (String sample : samplesParam.split(",")) {
                Integer sampleId = metadataManager.getSampleId(study, sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, study);
                }
                sampleIds.add(sampleId);
            }
            samples = sampleIds.stream().mapToInt(Integer::intValue).toArray();
            if (samples.length == 0) {
                throw new IllegalArgumentException("empty samples!");
            }
        }

        sampleIds = new TreeSet<>(Integer::compareTo);

        if (allSamples) {
            sampleIds.addAll(metadataManager.getIndexedSamples(study));
        } else {
            for (int sample : samples) {
                sampleIds.add(sample);
            }
        }
        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("empty samples!");
        }

        sampleIdToFileIdMap = new HashMap<>();
        for (Integer sampleId : sampleIds) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(study, sampleId);
            sampleIdToFileIdMap.put(sampleMetadata.getId(), sampleMetadata.getFiles());
        }

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        fixedAttributes = HBaseToVariantConverter.getFixedAttributes(studyMetadata);

        ObjectMap attributes = studyMetadata.getAttributes();
        hasGenotype = HBaseToVariantConverter.getFixedFormat(attributes).contains(VCFConstants.GENOTYPE_KEY);

        if (hasGenotype) {
            LOGGER.info("Study with genotypes : " + HBaseToVariantConverter.getFixedFormat(attributes));
        } else {
            LOGGER.info("Study without genotypes : " + HBaseToVariantConverter.getFixedFormat(attributes));
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String table) throws IOException {
        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                new QualifierFilter(EQUAL, new BinaryPrefixComparator(Bytes.toBytes(VariantPhoenixHelper.buildStudyColumnsPrefix(study)))),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'0', '|', '0', SEPARATOR_BYTE})),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'0', '/', '0', SEPARATOR_BYTE})),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'.', '/', '.', SEPARATOR_BYTE})),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'.', '|', '.', SEPARATOR_BYTE})),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'.', SEPARATOR_BYTE}))
        );

        if (secondaryOnly) {
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'0', '/', '1', SEPARATOR_BYTE})));
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'1', '/', '1', SEPARATOR_BYTE})));
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'1', '/', '2', SEPARATOR_BYTE})));
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'1', SEPARATOR_BYTE})));
        }

        List<Scan> scans;
        if (multiScan) {
            // FIXME: This will fail for large number of samples. Has to be fixed to use filters instead of explicit columns
            double numScans = Math.ceil(sampleIds.size() / partialScanSize);
            int samplesPerScan = (int) Math.ceil(sampleIds.size() / numScans);
            scans = new ArrayList<>((int) numScans);
            Scan scan = null;
            int samplesCount = 0;
            for (int sample : sampleIds) {
                if (samplesCount % samplesPerScan == 0) {
                    samplesCount = 0;
                    scan = new Scan();
                    if (StringUtils.isNotEmpty(region)) {
                        VariantHBaseQueryParser.addRegionFilter(scan, Region.parseRegion(region));
                    }
                    scan.setFilter(filter);
                    scans.add(scan);
                }
                byte[] sampleColumn = VariantPhoenixHelper.buildSampleColumnKey(study, sample);
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, sampleColumn);
                for (Integer fileId : sampleIdToFileIdMap.get(sample)) {
                    byte[] fileColumn = VariantPhoenixHelper.buildFileColumnKey(study, fileId);
                    scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, fileColumn);
                }
                samplesCount++;
            }
            // TODO: PartialResults may be an interesting feature, but is not available in v1.1.2. See [HBASE-14696] for more information
//        scan.setAllowPartialResults(true);

            if (scans.size() != numScans) {
                throw new IllegalArgumentException("Wrong number of scans. Expected " + numScans + " got " + scans.size());
            }
        } else {
            Scan scan = new Scan();
            if (StringUtils.isNotEmpty(region)) {
                VariantHBaseQueryParser.addRegionFilter(scan, Region.parseRegion(region));
            }
            scan.setFilter(filter);

            if (sampleIds.size() < 6000) {
                for (Integer sample : sampleIds) {
                    byte[] sampleColumn = VariantPhoenixHelper.buildSampleColumnKey(study, sample);
                    scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, sampleColumn);
                    for (Integer fileId : sampleIdToFileIdMap.get(sample)) {
                        byte[] fileColumn = VariantPhoenixHelper.buildFileColumnKey(study, fileId);
                        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, fileColumn);
                    }
                }
            }
            scans = Collections.singletonList(scan);
        }
        VariantMapReduceUtil.configureMapReduceScans(scans, getConf());

        for (int i = 0; i < scans.size(); i++) {
            Scan s = scans.get(i);
            LOGGER.info("scan[" + i + "]= " + s.toJSON());
        }

        try {
            VariantMapReduceUtil.initTableMapperJob(job, table, scans, SampleIndexerMapper.class);
            Class<? extends InputFormat<?, ?>> delegatedInputFormatClass = job.getInputFormatClass();
            job.setInputFormatClass(VariantAlignedInputFormat.class);
            VariantAlignedInputFormat.setDelegatedInputFormat(job, delegatedInputFormatClass);
            VariantAlignedInputFormat.setBatchSize(job, SampleIndexSchema.BATCH_SIZE);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        VariantMapReduceUtil.setOutputHBaseTable(job, outputTable);
        VariantMapReduceUtil.setNoneReduce(job);

//        job.setSpeculativeExecution(false);
        job.getConfiguration().setInt(MRJobConfig.TASK_TIMEOUT, 20 * 60 * 1000);

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, List<Integer>> entry : sampleIdToFileIdMap.entrySet()) {
            sb.append(entry.getKey()).append(':');
            Iterator<Integer> fileIt = entry.getValue().iterator();
            sb.append(fileIt.next());
            while (fileIt.hasNext()) {
                sb.append('_').append(fileIt.next());
            }
            sb.append(',');
        }
        job.getConfiguration().setBoolean(SampleIndexerMapper.HAS_GENOTYPE, hasGenotype);
        job.getConfiguration().set(SAMPLE_ID_TO_FILE_ID_MAP, sb.toString());
        job.getConfiguration().set(FIXED_ATTRIBUTES, String.join(",", fixedAttributes));
        if (allSamples) {
            job.getConfiguration().unset(SAMPLES);
        } else {
            job.getConfiguration().set(SAMPLES, sampleIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        }

        return job;
    }

    @Override
    protected void preExecution() throws IOException, StorageEngineException {
        super.preExecution();

        ObjectMap options = new ObjectMap();
        options.putAll(getParams());
        SampleIndexSchema.createTableIfNeeded(outputTable, getHBaseManager(), options);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new SampleIndexDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + SampleIndexDriver.class, e);
            System.exit(1);
        }
    }

    public static class SampleIndexerMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

        private static final String HAS_GENOTYPE = "SampleIndexerMapper.hasGenotype";
        public static final int SAMPLES_TO_COUNT = 2;
        private byte[] family;
        private Set<Integer> samplesSet;
        private Set<Integer> samplesToCount;
        private SampleIndexVariantBiConverter variantsConverter;
        private VariantFileIndexConverter fileIndexConverter;
        private List<String> fixedAttributes;
        private Map<Integer, List<Integer>> sampleIdToFileIdMap;
        private boolean hasGenotype;

        private Map<Integer, Map<String, ByteArrayOutputStream>> sampleGtMap = new HashMap<>();
        private Map<Integer, Map<String, Integer>> sampleGtCountMap = new HashMap<>();
        private Map<Integer, Map<String, ByteArrayOutputStream>> sampleFileIndexMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            new GenomeHelper(context.getConfiguration());
            family = GenomeHelper.COLUMN_FAMILY_BYTES;
            hasGenotype = context.getConfiguration().getBoolean(HAS_GENOTYPE, true);
            fileIndexConverter = new VariantFileIndexConverter();

            int[] samples = context.getConfiguration().getInts(SAMPLES);
            if (samples == null || samples.length == 0) {
                samplesSet = null;
                samplesToCount = new HashSet<>(SAMPLES_TO_COUNT);
                for (int i = 0; i < SAMPLES_TO_COUNT; i++) {
                    samplesToCount.add(i + 1);
                }
            } else {
                samplesSet = new HashSet<>(samples.length);
                IntStream.of(samples).forEach(samplesSet::add);

                samplesToCount = new HashSet<>(SAMPLES_TO_COUNT);
                for (int i = 0; i < Math.min(samples.length, SAMPLES_TO_COUNT); i++) {
                    samplesToCount.add(samples[i]);
                }
            }

            variantsConverter = new SampleIndexVariantBiConverter();

            String[] strings = context.getConfiguration().getStrings(FIXED_ATTRIBUTES);
            if (strings != null) {
                fixedAttributes = Arrays.asList(strings);
            } else {
                fixedAttributes = Collections.emptyList();
            }
            sampleIdToFileIdMap = new HashMap<>();
            String s = context.getConfiguration().get(SAMPLE_ID_TO_FILE_ID_MAP);
            for (String sampleFiles : s.split(",")) {
                if (!sampleFiles.isEmpty()) {
                    String[] sampleFilesSplit = sampleFiles.split(":");
                    Integer sampleId = Integer.valueOf(sampleFilesSplit[0]);
                    String[] files = sampleFilesSplit[1].split("_");
                    sampleIdToFileIdMap.put(sampleId, Arrays.stream(files).map(Integer::valueOf).collect(Collectors.toList()));
                }
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());

            // Get fileIndex for each file
            Map<Integer, Short> fileIndexMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                Integer fileId = VariantPhoenixHelper
                        .extractFileIdOrNull(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if (fileId != null) {
                    PhoenixArray fileColumn = (PhoenixArray)
                            PVarcharArray.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    Map<String, String> fileAttributes = HBaseToStudyEntryConverter.convertFileAttributes(fileColumn, fixedAttributes);

                    // FIXME!
                    short fileIndexValue = fileIndexConverter.createFileIndexValue(variant.getType(), -1, fileAttributes, null);

                    fileIndexMap.put(fileId, fileIndexValue);
                }
            }

            for (Cell cell : result.rawCells()) {
                Integer sampleId = VariantPhoenixHelper
                        .extractSampleIdOrNull(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if (sampleId != null && (samplesSet == null || samplesSet.contains(sampleId))) {
                    String gt;
                    boolean validGt;
                    if (hasGenotype) {
                        ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                                cell.getValueArray(),
                                cell.getValueOffset(),
                                cell.getValueLength());
                        PhoenixHelper.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, null);
                        if (ptr.getLength() == 0) {
                            gt = GenotypeClass.NA_GT_VALUE;
                            validGt = true;
                        } else {
                            gt = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());
                            validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                        }
                    } else {
                        gt = GenotypeClass.NA_GT_VALUE;
                        validGt = true;
                    }
                    if (validGt) {
                        ByteArrayOutputStream stream = sampleGtMap
                                .computeIfAbsent(sampleId, k -> new HashMap<>())
                                .computeIfAbsent(gt, k -> new ByteArrayOutputStream(2000));
                        variantsConverter.toBytes(variant, stream);

                        // Increase counters
                        sampleGtCountMap.computeIfAbsent(sampleId, k -> new HashMap<>()).merge(gt, 1, Integer::sum);

                        // Add fileIndex value for this genotype
                        Short fileIndex = null;
                        for (Integer fileId : sampleIdToFileIdMap.get(sampleId)) {
                            fileIndex = fileIndexMap.get(fileId);
                            if (fileIndex != null) {
                                break;
                            }
                        }
                        if (fileIndex == null) {
                            throw new IllegalStateException("File " + sampleIdToFileIdMap.get(sampleId) + " not found for sample "
                                    + sampleId + " in variant " + variant);
                        }
                        sampleFileIndexMap
                                .computeIfAbsent(sampleId, k -> new HashMap<>())
                                .computeIfAbsent(gt, k -> new ByteArrayOutputStream(50)).write(fileIndex);


                        if (samplesToCount.contains(sampleId)) {
                            context.getCounter(COUNTER_GROUP_NAME, "SAMPLE_" + sampleId + '_' + gt).increment(1);
                        }
                    }
                }
            }

        }

        @Override
        public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {

            for (Map.Entry<Integer, Map<String, ByteArrayOutputStream>> entry : sampleGtMap.entrySet()) {
                Integer sampleId = entry.getKey();
                Map<String, ByteArrayOutputStream> gts = entry.getValue();
                Map<String, Integer> gtsCount = sampleGtCountMap.get(sampleId);
                Map<String, ByteArrayOutputStream> fileIndex = sampleFileIndexMap.get(sampleId);

                Put put = new Put(SampleIndexSchema.toRowKey(sampleId, chromosome, position));

                gts.forEach((gt,  stream) -> {
                    if (stream.size() > 0) {
                        // Copy byte array, as the ByteArrayOutputStream will be reset and reused!
                        put.addColumn(family, SampleIndexSchema.toGenotypeColumn(gt), stream.toByteArray());
                        stream.reset();
                    }
                });

                gtsCount.forEach((gt, count) -> {
                    if (count > 0) {
                        put.addColumn(family, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(count));
                    }
                });
                gtsCount.clear();

                fileIndex.forEach((gt, stream) -> {
                    if (stream.size() > 0) {
                        // Copy byte array, as the ByteArrayOutputStream will be reset and reused!
                        put.addColumn(family, SampleIndexSchema.toFileIndexColumn(gt), stream.toByteArray());
                        stream.reset();
                    }
                });

                if (put.isEmpty()) {
                    context.getCounter(COUNTER_GROUP_NAME, "empty_put").increment(1);
                } else {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
                }
            }
        }
    }

}
