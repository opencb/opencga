package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created on 03/10/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SaturationStatsDriver extends AbstractVariantsTableDriver {
    private static final Logger LOG = LoggerFactory.getLogger(SaturationStatsDriver.class);
    protected static final String BATCH_SIZE = "--batch-size";
    protected static final String OUTPUT_FILE = "--output";
    protected static final String NUM_BATCHES = "num_batches";
    private String region;
    private int batchSize;
    private String outputPath;
    private int caching;

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + HadoopVariantStorageEngine.STUDY_ID, "<integer>*");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        params.put(OUTPUT_FILE, "<path>");
        params.put(BATCH_SIZE, "<integer>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        region = getConf().get(VariantQueryParam.REGION.key(), getConf().get("--" + VariantQueryParam.REGION.key()));
        batchSize = getConf().getInt(BATCH_SIZE, 1000);
        outputPath = getConf().get(OUTPUT_FILE);
        if (StringUtils.isEmpty(outputPath)) {
            outputPath = "saturation." + TimeUtils.getTime() + ".json";
        }
        caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        StudyConfiguration sc = getMetadataManager().getStudyConfiguration(getStudyId(), null).first();

        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setCacheBlocks(false);
        if (StringUtils.isNotEmpty(region)) {
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        int numSamples = sc.getSampleIds().size();
        for (Integer sampleId : sc.getSampleIds().values()) {
            scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.buildSampleColumnKey(sc.getStudyId(), sampleId));
        }
        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.VariantColumn.TYPE.bytes());

//        scan.setFilter(new KeyOnlyFilter());
        scan.setFilter(
                new FilterList(FilterList.Operator.MUST_PASS_ALL,
                        new FilterList(FilterList.Operator.MUST_PASS_ONE,
                                new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("0/1"))),
                                new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("1"))),
                                // Include TYPE, so we can count variants not in any sample
                                new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                                        new BinaryComparator(VariantPhoenixHelper.VariantColumn.TYPE.bytes()))
                        ),
                        new KeyOnlyFilter()
                )
        );

        LOG.info("scan = " + scan.toString(10));

        int numBatches = (int) Math.ceil((float) numSamples / batchSize) + 1;
        LOG.info("Batch size: " + batchSize);
        LOG.info("Num batches: " + numBatches);

        job.getConfiguration().setInt(BATCH_SIZE, batchSize);
        job.getConfiguration().setInt(NUM_BATCHES, numBatches);


        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, table, scan, SaturationStatsMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(SaturationStatsWritable.class);

        job.setCombinerClass(SaturationStatsCombiner.class);

        job.setReducerClass(SaturationStatsReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
//        job.setSpeculativeExecution(false);
//        job.getConfiguration().setInt(MRJobConfig.TASK_TIMEOUT, 20 * 60 * 1000);


        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        LOG.info("Output file: " + outputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);

        return job;
    }

    @Override
    protected Class<?> getMapperClass() {
        return SaturationStatsMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return "saturation_stats";
    }

    public static class SaturationStatsWritable implements Writable {
        private int[] variantsInBatch;
        private int[] variantsTotal;
        private int variantsInNoSample;
//        int[] samplesPerBatch;

        public SaturationStatsWritable() {
            init(0);
        }

        public SaturationStatsWritable(int numBatches) {
            init(numBatches);
        }

        public void init(int numBatches) {
            variantsInBatch = new int[numBatches];
            variantsTotal = new int[numBatches];
            variantsInNoSample = 0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(variantsInBatch.length);
            for (int i : variantsInBatch) {
                out.writeInt(i);
            }
            for (int i : variantsTotal) {
                out.writeInt(i);
            }
            out.writeInt(variantsInNoSample);
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            int numBatches = input.readInt();
            init(numBatches);
            for (int i = 0; i < numBatches; i++) {
                variantsInBatch[i] = input.readInt();
            }
            for (int i = 0; i < numBatches; i++) {
                variantsTotal[i] = input.readInt();
            }
            variantsInNoSample = input.readInt();
        }

        public void merge(SaturationStatsWritable other) {
            for (int i = 0; i < other.variantsInBatch.length; i++) {
                this.variantsInBatch[i] += other.variantsInBatch[i];
            }
            for (int i = 0; i < other.variantsTotal.length; i++) {
                this.variantsTotal[i] += other.variantsTotal[i];
            }
            this.variantsInNoSample += other.variantsInNoSample;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("variantsPerBatch", variantsInBatch)
                    .append("variantsPerBatchAggregation", variantsTotal)
                    .toString();
        }

    }

    public static class SaturationStatsMapper extends TableMapper<NullWritable, SaturationStatsWritable> {
        private int numBatches;
        private int batchSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numBatches = context.getConfiguration().getInt(NUM_BATCHES, 0);
            batchSize = context.getConfiguration().getInt(BATCH_SIZE, 0);
            super.setup(context);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            SaturationStatsWritable stats = new SaturationStatsWritable(numBatches);

            int samples = 0;
            for (Cell cell : value.rawCells()) {
                String column = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                Integer sampleId = VariantPhoenixHelper.extractSampleId(column, false);
                if (sampleId != null) {
                    samples++;
                    int batch = sampleId / batchSize;
                    stats.variantsInBatch[batch] = 1;
//                    stats.samplesPerBatch[batch]++;
                }
            }
            if (samples == 0) {
                // Variant in no sample!
                stats.variantsInNoSample++;
            } else {
                int firstBatchWithVariants = 0;
                for (int i = 0; i < stats.variantsInBatch.length; i++) {
                    if (stats.variantsInBatch[i] == 1) {
                        firstBatchWithVariants = i;
                        break;
                    }
                }

                for (int i = firstBatchWithVariants; i < stats.variantsInBatch.length; i++) {
                    stats.variantsTotal[i] = 1;
                }
            }

            context.write(NullWritable.get(), stats);
        }
    }

    public static class SaturationStatsCombiner extends Reducer<NullWritable, SaturationStatsWritable,
            NullWritable, SaturationStatsWritable> {

        private int numBatches;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numBatches = context.getConfiguration().getInt(NUM_BATCHES, 0);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<SaturationStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            SaturationStatsWritable stats = new SaturationStatsWritable(numBatches);

            for (SaturationStatsWritable partialStats : values) {
                stats.merge(partialStats);
            }

            context.write(key, stats);
        }
    }

    public static class SaturationStatsReducer extends Reducer<NullWritable, SaturationStatsWritable,
            NullWritable, Text> {

        private int numBatches;
        private int batchSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numBatches = context.getConfiguration().getInt(NUM_BATCHES, 0);
            batchSize = context.getConfiguration().getInt(BATCH_SIZE, 0);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<SaturationStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            SaturationStatsWritable stats = new SaturationStatsWritable(numBatches);

            for (SaturationStatsWritable partialStats : values) {
                stats.merge(partialStats);
            }

            StringBuilder sb = new StringBuilder()
                    .append("##Variants in no sample = ").append(stats.variantsInNoSample).append('\n')
                    .append("#LOADED_SAMPLES\tVARIANTS_IN_BATCH\tLOADED_VARIANTS\tNEW_VARIANTS\n");

            int prevNumVariants = 0;
            for (int i = 0; i < numBatches; i++) {
                sb.append((i + 1) * batchSize).append('\t');
                sb.append(stats.variantsInBatch[i]).append('\t');
                sb.append(stats.variantsTotal[i]).append('\t');
                sb.append(stats.variantsTotal[i] - prevNumVariants).append('\n');
                prevNumVariants = stats.variantsTotal[i];
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            System.exit(new SaturationStatsDriver().privateMain(args));
        } catch (Exception e) {
            LOG.error("Error executing " + SaturationStatsDriver.class, e);
            System.exit(1);
        }
    }

}
