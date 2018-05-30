package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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
public class SampleIndexDriver extends AbstractAnalysisTableDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleIndexDriver.class);
    private int study;
    private int[] samples;
    private String outputTable;
    private boolean allSamples;
    private boolean secondaryOnly;

    @Override
    protected String getJobOperationName() {
        return "sample_index";
    }

    @Override
    protected void parseAndValidateParameters() {
        outputTable = getConf().get("output");
        if (outputTable == null || outputTable.isEmpty()) {
            throw new IllegalArgumentException("Missing output table!");
        }
        study = getStudyId();

        secondaryOnly = getConf().getBoolean("secondaryOnly", false);

        if (getConf().get("samples").equals(VariantQueryUtils.ALL)) {
            allSamples = true;
            samples = null;
        } else {
            allSamples = false;
            samples = getConf().getInts("samples");
            if (samples == null || samples.length == 0) {
                throw new IllegalArgumentException("empty samples!");
            }
        }

    }

    @Override
    protected Class<?> getMapperClass() {
        return SampleIndexerMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String table) throws IOException {
        int caching = job.getConfiguration().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 100);
        LOGGER.info("Scan set Caching to " + caching);

        TreeSet<Integer> sampleIds = new TreeSet<>(Integer::compareTo);


        if (allSamples) {
            StudyConfiguration sc = getStudyConfigurationManager().getStudyConfiguration(study, null).first();
            for (Integer fileId : sc.getIndexedFiles()) {
                sampleIds.addAll(sc.getSamplesInFiles().get(fileId));
            }
        } else {
            for (int sample : samples) {
                sampleIds.add(sample);
            }
        }
        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("empty samples!");
        }

        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'0', '/', '0', SEPARATOR_BYTE})),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'.', '/', '.', SEPARATOR_BYTE})),
                new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'.', SEPARATOR_BYTE}))
        );

        if (secondaryOnly) {
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'0', '/', '1', SEPARATOR_BYTE})));
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'1', '/', '1', SEPARATOR_BYTE})));
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'1', '/', '2', SEPARATOR_BYTE})));
            filter.addFilter(new ValueFilter(NOT_EQUAL, new BinaryPrefixComparator(new byte[]{'1', SEPARATOR_BYTE})));
        }

        double partialScanSize = 1000; // Max number of samples to be processed in each Scan.
        double numScans = Math.ceil(sampleIds.size() / partialScanSize);
        int samplesPerScan = (int) Math.ceil(sampleIds.size() / numScans);
        List<Scan> scans = new ArrayList<>((int) numScans);
        Scan scan = null;
        int samplesCount = 0;
        for (int sample : sampleIds) {
            if (samplesCount % samplesPerScan == 0) {
                samplesCount = 0;
                scan = new Scan();
                scan.setCaching(caching);        // 1 is the default in Scan
                scan.setCacheBlocks(false);  // don't set to true for MR jobs
                String region = getConf().get("region");
                if (region != null && !region.isEmpty()) {
                    VariantHBaseQueryParser.addRegionFilter(scan, Region.parseRegion(region));
                }
                scans.add(scan);
            }
            byte[] column = VariantPhoenixHelper.buildSampleColumnKey(study, sample);
            scan.addColumn(getHelper().getColumnFamily(), column);
            scan.setFilter(filter);
            samplesCount++;
        }
        // TODO: PartialResults may be an interesting feature, but is not available in v1.1.2. See [HBASE-14696] for more information
//        scan.setAllowPartialResults(true);

        if (scans.size() != numScans) {
            throw new IllegalArgumentException("Wrong number of scans. Expected " + numScans + " got " + scans.size());
        }

        for (int i = 0; i < scans.size(); i++) {
            Scan s = scans.get(i);
            LOGGER.info("scan[" + i + "]= " + s.toJSON());
        }

        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, table, scans, SampleIndexerMapper.class);
        VariantMapReduceUtil.setOutputHBaseTable(job, outputTable);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(GtVariantsWritable.class);
        job.setCombinerClass(SampleIndexerCombiner.class);
        TableMapReduceUtil.initTableReducerJob(outputTable, SampleIndexerReducer.class, job);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Mutation.class);

        job.setSpeculativeExecution(false);
        job.getConfiguration().setInt(MRJobConfig.TASK_TIMEOUT, 20 * 60 * 1000);

        // TODO: Can we use HFileOutputFormat2 here?

        return job;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new SampleIndexDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + SampleIndexDriver.class, e);
            System.exit(1);
        }
    }

    public int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf != null) {
            setConf(conf);
        }
        return ToolRunner.run(this, args);
    }

    public static class GtVariantsWritable extends ArrayWritable {

        public GtVariantsWritable() {
            super(Text.class);
        }

        public GtVariantsWritable(String gt, String variants) {
            this();
            super.set(new Writable[]{new Text(gt), new Text(variants)});
        }

        public String getGt() {
            return get()[0].toString();
        }

        public String getVariants() {
            return get()[1].toString();
        }

    }

    public static class SampleIndexerMapper extends TableMapper<ImmutableBytesWritable, GtVariantsWritable> {

        private Set<Integer> samplesToCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            int[] samples = context.getConfiguration().getInts("samples");
            samplesToCount = new HashSet<>(5);
            for (int i = 0; i < Math.min(samples.length, 5); i++) {
                samplesToCount.add(samples[i]);
            }
        }

        @Override
        protected void map(ImmutableBytesWritable k, Result result, Context context) throws IOException, InterruptedException {
            Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());

            for (Cell cell : result.rawCells()) {
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                int sampleId = Integer.valueOf(column.split("_")[1]);

//                PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(
//                        cell.getValueArray(),
//                        cell.getValueOffset(),
//                        cell.getValueLength());
//
//                String gt = array.getElement(0).toString();

                byte[] valueArray = cell.getValueArray();
                int gtLength = 0;
                for (int i = cell.getValueOffset(); i < valueArray.length; i++) {
                    if (valueArray[i] == SEPARATOR_BYTE) {
                        break;
                    }
                    gtLength++;
                }
                String gt = Bytes.toString(valueArray, cell.getValueOffset(), gtLength);

                if (SampleIndexDBLoader.validGenotype(gt)) {
                    ImmutableBytesWritable key = new ImmutableBytesWritable(
                            SampleIndexConverter.toRowKey(sampleId, variant.getChromosome(), variant.getStart()));
                    GtVariantsWritable value = new GtVariantsWritable(gt, variant.toString());
                    context.write(key, value);
                    if (samplesToCount.contains(sampleId)) {
                        context.getCounter("SAMPLE_INDEX", "SAMPLE_" + sampleId + '_' + gt).increment(1);
                    }
                }
            }
        }
    }

    public static class SampleIndexerCombiner extends Reducer<ImmutableBytesWritable, GtVariantsWritable,
            ImmutableBytesWritable, GtVariantsWritable> {

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<GtVariantsWritable> values, Context context)
                throws IOException, InterruptedException {
            Map<String, StringBuilder> gtsMap = new HashMap<>();
            for (GtVariantsWritable value : values) {
                String gt = value.getGt();
                String variant = value.getVariants();
                gtsMap.computeIfAbsent(gt, k -> new StringBuilder()).append(variant).append(',');
            }

            for (Map.Entry<String, StringBuilder> entry : gtsMap.entrySet()) {
                context.write(key, new GtVariantsWritable(entry.getKey(), entry.getValue().toString()));
            }
        }
    }

    public static class SampleIndexerReducer extends TableReducer<ImmutableBytesWritable, GtVariantsWritable, ImmutableBytesWritable> {

        private byte[] family;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            family = new GenomeHelper(context.getConfiguration()).getColumnFamily();
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<GtVariantsWritable> values, Context context)
                throws IOException, InterruptedException {
            Map<String, StringBuilder> gtsMap = new HashMap<>();
            for (GtVariantsWritable value : values) {
                String gt = value.getGt();
                String variant = value.getVariants();
                gtsMap.computeIfAbsent(gt, k -> new StringBuilder()).append(variant).append(',');
            }
            Put put = new Put(key.get());
            put.setDurability(Durability.SKIP_WAL);
            for (Map.Entry<String, StringBuilder> entry : gtsMap.entrySet()) {
                put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }
            context.getCounter("SAMPLE_INDEX", "PUT").increment(1);

            context.write(key, put);
        }
    }

}
