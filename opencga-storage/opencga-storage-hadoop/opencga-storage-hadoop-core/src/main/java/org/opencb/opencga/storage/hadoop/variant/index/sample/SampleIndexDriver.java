package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.BiMap;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
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
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.VariantTableSampleIndexOrderMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
    public static final String SAMPLE_IDS = "sampleIds";
    public static final String OUTPUT = "output";
    public static final String SECONDARY_ONLY = "secondary-only";
//    public static final String MAIN_ONLY = "main-only";
    public static final String PARTIAL_SCAN_SIZE = "partial-scan-size";
    public static final String MAX_COLUMNS_PER_SCAN = "max-columns-per-scan";

    private static final String SAMPLE_ID_TO_FILE_ID_MAP = "SampleIndexDriver.sampleIdToFileIdMap";
    private static final String MULTI_FILE_SAMPLES = "SampleIndexDriver.multiFileSamples";
    private static final String FIXED_ATTRIBUTES = "SampleIndexDriver.fixedAttributes";
    private static final String FIXED_SAMPLE_DATA_KEYS = "SampleIndexDriver.fixedSampleDataKeys";
    private static final String PARTIAL_SCAN = "SampleIndexDriver.partial_scan";

    private int study;
    private String outputTable;
    private boolean allSamples;
    private boolean secondaryOnly;
    private boolean mainOnly;
    private boolean hasGenotype;
    private TreeSet<Integer> sampleIds;
    private Map<Integer, List<Integer>> sampleIdToFileIdMap;
    private Set<Integer> multiFileSamples;
    private String region;
    private double partialScanSize;
    private List<String> fixedAttributes;
    private List<String> fixedFormat;
    private boolean multiScan = false;
    private int maxColumns;

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
        params.put("--" + SAMPLES, "<samples>");
        params.put("--" + SAMPLE_IDS, "<sample-ids>");
        params.put("--" + VariantStorageOptions.STUDY.key(), "<study>");
        params.put("--" + OUTPUT, "<output-table>");
        params.put("--" + SECONDARY_ONLY, "<true|false>");
//        params.put("--" + MAIN_ONLY, "<main-alternate-only>");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        params.put("--" + PARTIAL_SCAN_SIZE, "<samples-per-scan>");
        params.put("--" + MAX_COLUMNS_PER_SCAN, "<max-columns-per-scan>");
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
        maxColumns = Integer.valueOf(getParam(MAX_COLUMNS_PER_SCAN, "4000"));

        String samplesParam = getParam(SAMPLES);
        String sampleIdsStr = getParam(SAMPLE_IDS);
        sampleIds = new TreeSet<>(Integer::compareTo);
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        if (StringUtils.isNotEmpty(samplesParam) && StringUtils.isNotEmpty(sampleIdsStr)) {
            throw new IllegalArgumentException("Incompatible params " + SAMPLES + " and " + SAMPLE_IDS);
        }
        if (VariantQueryUtils.ALL.equals(samplesParam)) {
            allSamples = true;
            sampleIds.addAll(metadataManager.getIndexedSamples(study));
        } else if (StringUtils.isNotEmpty(samplesParam)) {
            allSamples = false;
            for (String sample : samplesParam.split(",")) {
                Integer sampleId = metadataManager.getSampleId(getStudyId(), sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, study);
                }
                sampleIds.add(sampleId);
            }
        } else if (StringUtils.isNotEmpty(sampleIdsStr)) {
            allSamples = false;
            for (String sample : sampleIdsStr.split(",")) {
                sampleIds.add(Integer.valueOf(sample));
            }
        }

        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("empty samples!");
        }

        sampleIdToFileIdMap = new HashMap<>();
        multiFileSamples = new HashSet<>();
        for (Integer sampleId : sampleIds) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(study, sampleId);
            sampleIdToFileIdMap.put(sampleMetadata.getId(), sampleMetadata.getFiles());
            if (sampleMetadata.isMultiFileSample()) {
                multiFileSamples.add(sampleId);
            }
        }

        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        fixedAttributes = HBaseToVariantConverter.getFixedAttributes(studyMetadata);

        fixedFormat = HBaseToVariantConverter.getFixedFormat(studyMetadata);
        hasGenotype = fixedFormat.contains(VCFConstants.GENOTYPE_KEY);

        if (hasGenotype) {
            LOGGER.info("Study with genotypes : " + fixedFormat);
        } else {
            LOGGER.info("Study without genotypes : " + fixedFormat);
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String table) throws IOException {
        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                new QualifierFilter(EQUAL, new BinaryPrefixComparator(Bytes.toBytes(VariantPhoenixSchema.buildStudyColumnsPrefix(study)))),
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
            for (List<Integer> samplesSubSet : sampleIds.stream().collect(Collectors.groupingBy(s -> s / samplesPerScan)).values()) {
                Scan scan = new Scan();
                if (StringUtils.isNotEmpty(region)) {
                    VariantHBaseQueryParser.addRegionFilter(scan, Region.parseRegion(region));
                }
                scan.setFilter(filter);
                scans.add(scan);
                for (int sample : samplesSubSet) {
                    byte[] sampleColumn = VariantPhoenixSchema.buildSampleColumnKey(study, sample);
                    scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, sampleColumn);
                    for (Integer fileId : sampleIdToFileIdMap.get(sample)) {
                        if (multiFileSamples.contains(sample)) {
                            byte[] sampleFileColumn = VariantPhoenixSchema.buildSampleColumnKey(study, sample, fileId);
                            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, sampleFileColumn);
                        }
                        byte[] fileColumn = VariantPhoenixSchema.buildFileColumnKey(study, fileId);
                        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, fileColumn);
                    }
                }
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

            int approxExpectedNumColumns =
                    sampleIds.size()
                    + sampleIdToFileIdMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()).size();
            if (approxExpectedNumColumns < maxColumns) {
                for (Integer sample : sampleIds) {
                    byte[] sampleColumn = VariantPhoenixSchema.buildSampleColumnKey(study, sample);
                    scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, sampleColumn);
                    for (Integer fileId : sampleIdToFileIdMap.get(sample)) {
                        if (multiFileSamples.contains(sample)) {
                            byte[] sampleFileColumn = VariantPhoenixSchema.buildSampleColumnKey(study, sample, fileId);
                            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, sampleFileColumn);
                        }
                        byte[] fileColumn = VariantPhoenixSchema.buildFileColumnKey(study, fileId);
                        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, fileColumn);
                    }
                }
            } else {
                job.getConfiguration().setBoolean(PARTIAL_SCAN, true);
            }
            scans = Collections.singletonList(scan);
        }
        VariantMapReduceUtil.configureMapReduceScans(scans, getConf());

        for (int i = 0; i < scans.size(); i++) {
            Scan s = scans.get(i);
            LOGGER.info("scan[" + i + "]= " + s.toJSON(20));
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
        job.getConfiguration().set(MULTI_FILE_SAMPLES, multiFileSamples.stream().map(Object::toString).collect(Collectors.joining(",")));
        job.getConfiguration().set(FIXED_ATTRIBUTES, String.join(",", fixedAttributes));
        job.getConfiguration().set(FIXED_SAMPLE_DATA_KEYS, String.join(",", fixedFormat));
        if (allSamples) {
            job.getConfiguration().unset(SAMPLES);
        } else {
            job.getConfiguration().set(SAMPLES, sampleIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        }

        StudyMetadata studyMetadata = getMetadataManager().getStudyMetadata(getStudyId());
        VariantMapReduceUtil.setSampleIndexConfiguration(job, studyMetadata.getSampleIndexConfigurationLatest().getConfiguration());

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
        private Set<Integer> samplesToCount;
        private VariantFileIndexConverter fileIndexConverter;
        private List<String> fixedAttributes;
        private Map<String, Integer> sampleDataKeyPositions;
        private final Map<Integer, SampleMetadata> samples = new HashMap<>();
        private final Set<Integer> files = new HashSet<>();
        private boolean hasGenotype;
        private SampleIndexSchema schema;

        private final Map<Integer, SampleIndexEntryPutBuilder> samplesMap = new HashMap<>();
        private boolean partialScan;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            new GenomeHelper(context.getConfiguration());
            hasGenotype = context.getConfiguration().getBoolean(HAS_GENOTYPE, true);
            schema = new SampleIndexSchema(VariantMapReduceUtil.getSampleIndexConfiguration(context.getConfiguration()));
            fileIndexConverter = new VariantFileIndexConverter(schema);

            int[] sampleIds = context.getConfiguration().getInts(SAMPLES);
            if (sampleIds == null || sampleIds.length == 0) {
                samplesToCount = new HashSet<>(SAMPLES_TO_COUNT);
                for (int i = 0; i < SAMPLES_TO_COUNT; i++) {
                    samplesToCount.add(i + 1);
                }
            } else {
                samplesToCount = new HashSet<>(SAMPLES_TO_COUNT);
                for (int i = 0; i < Math.min(sampleIds.length, SAMPLES_TO_COUNT); i++) {
                    samplesToCount.add(sampleIds[i]);
                }
            }

            String[] strings = context.getConfiguration().getStrings(FIXED_ATTRIBUTES);
            if (strings != null) {
                fixedAttributes = Arrays.asList(strings);
            } else {
                fixedAttributes = Collections.emptyList();
            }
            strings = context.getConfiguration().getStrings(FIXED_SAMPLE_DATA_KEYS);
            if (strings == null) {
                throw new IllegalStateException("Missing " + FIXED_SAMPLE_DATA_KEYS);
            } else {
                sampleDataKeyPositions = new HashMap<>();
                for (int i = 0; i < strings.length; i++) {
                    sampleDataKeyPositions.put(strings[i], i);
                }
            }
            String s = context.getConfiguration().get(SAMPLE_ID_TO_FILE_ID_MAP);
            for (String sampleFiles : s.split(",")) {
                if (!sampleFiles.isEmpty()) {
                    String[] sampleFilesSplit = sampleFiles.split(":");
                    int sampleId = Integer.parseInt(sampleFilesSplit[0]);
                    String[] files = sampleFilesSplit[1].split("_");
                    List<Integer> fileIds = new ArrayList<>(files.length);
                    for (String file : files) {
                        fileIds.add(Integer.valueOf(file));
                    }
                    this.files.addAll(fileIds);
                    samples.put(sampleId, new SampleMetadata(0, sampleId, null).setFiles(fileIds));
                }
            }
            partialScan = context.getConfiguration().getBoolean(PARTIAL_SCAN, false);
            for (int sampleId : context.getConfiguration().getInts(MULTI_FILE_SAMPLES)) {
                samples.get(sampleId).setSplitData(VariantStorageEngine.SplitData.MULTI);
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            VariantRow variantRow = new VariantRow(result);
            Variant variant = variantRow.getVariant();

            // Get fileIndex for each file
            Map<Integer, BitBuffer> fileIndexMap = new HashMap<>();

            variantRow.forEachFile(fileColumn -> {
                if (partialScan && !this.files.contains(fileColumn.getFileId())) {
                    // Discard extra files
                    // Only check map with a Partial Scan.
                    return;
                }
                Map<String, String> fileAttributes = HBaseToStudyEntryConverter.convertFileAttributes(fileColumn.raw(), fixedAttributes);

                BitBuffer fileIndexValue = fileIndexConverter.createFileIndexValue(variant.getType(), 0, fileAttributes,
                        Collections.emptyMap(), Collections.emptyList());

                fileIndexMap.put(fileColumn.getFileId(), fileIndexValue);
            });

            variantRow.forEachSample(sampleColumn -> {
                int sampleId = sampleColumn.getSampleId();
                SampleMetadata sampleMetadata = samples.get(sampleId);
                if (sampleMetadata == null) {
                    // Discard extra samples
                    return;
                }
                String gt;
                boolean validGt;
                if (hasGenotype) {
                    gt = sampleColumn.getGT();
                    if (gt == null || gt.isEmpty()) {
                        gt = GenotypeClass.NA_GT_VALUE;
                        validGt = true;
                    } else {
                        validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                    }
                } else {
                    gt = GenotypeClass.NA_GT_VALUE;
                    validGt = true;
                }
                if (validGt) {
                    SampleIndexEntryPutBuilder builder = samplesMap.computeIfAbsent(sampleId,
                            s -> new SampleIndexEntryPutBuilder(s, variant, schema));
                    List<Integer> files;
                    int filePosition;
                    if (sampleMetadata.isMultiFileSample()) {
                        Integer fileId = sampleColumn.getFileId();
                        if (fileId == null) {
                            files = Collections.singletonList(sampleMetadata.getFiles().get(0));
                            filePosition = 0;
                        } else {
                            files = Collections.singletonList(fileId);
                            filePosition = sampleMetadata.getFiles().indexOf(fileId);
                        }
                    } else {
                        files = sampleMetadata.getFiles();
                        filePosition = -1;
                    }

                    // Add fileIndex value for this genotype
                    boolean fileFound = false;
                    for (Integer fileId : files) {
                        BitBuffer fileIndex = fileIndexMap.get(fileId);
                        if (fileIndex != null) {
                            fileFound = true;
                            // Copy bit buffer
                            BitBuffer sampleFileIndex = new BitBuffer(fileIndex);
                            fileIndexConverter.addSampleDataIndexValues(sampleFileIndex, sampleDataKeyPositions,
                                    sampleColumn::getSampleData);
                            if (filePosition >= 0) {
                                schema.getFileIndex().getFilePositionIndex().write(filePosition, sampleFileIndex);
                            }
                            builder.add(gt, new SampleVariantIndexEntry(variant, sampleFileIndex));
                            countSampleGt(context, sampleId, gt);
                        }
                    }

                    if (!fileFound) {
                        throw new IllegalStateException("File " + files + " not found for sample " + sampleId + " in variant " + variant);
                    }
                }
            });

        }

        private void countSampleGt(Context context, int sampleId, String gt) {
            if (samplesToCount.contains(sampleId)) {
                switch (gt) {
                    case "1/1":
                    case "0/1":
                    case "1/2":
                    case "1/3":
                    case "0|1":
                    case "1|0":
                    case "1|1":
                        context.getCounter(COUNTER_GROUP_NAME, "SAMPLE_" + sampleId + "_" + gt).increment(1);
                        break;
                    default:
                        context.getCounter(COUNTER_GROUP_NAME, "SAMPLE_" + sampleId + "_x/x").increment(1);
                        break;
                }
            }
        }

        @Override
        public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {

            for (SampleIndexEntryPutBuilder builder : samplesMap.values()) {
                Put put = builder.build();

                if (put == null || put.isEmpty()) {
                    context.getCounter(COUNTER_GROUP_NAME, "empty_put").increment(1);
                } else {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
                }
            }
            samplesMap.clear();
        }
    }

}
