package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created on 15/05/18.
 *
 * <code>
 * export HADOOP_CLASSPATH=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
 * hadoop jar opencga-storage-hadoop-core-X.Y.Z-dev-jar-with-dependencies.jar
 *      org.opencb.opencga.storage.hadoop.variant.adaptors.sampleIndex.SampleIndexerDriver
 *      $VARIANTS_TABLE_NAME
 *      output $SAMPLE_INDEX_TABLE
 *      studyId $STUDY_ID
 *      samples $SAMPLE_IDS
 *      ....
 * </code>
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexerDriver extends AbstractHBaseDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleIndexerDriver.class);
    private int study;
    private int[] samples;
    private GenomeHelper genomeHelper;
    private String outputTable;

    @Override
    protected String getJobName() {
        return "sample_index";
    }

    @Override
    protected void parseAndValidateParameters() {
        super.parseAndValidateParameters();
        outputTable = getConf().get("output");
        if (outputTable == null || outputTable.isEmpty()) {
            throw new IllegalArgumentException("Missing output table!");
        }
        study = getConf().getInt("studyId", 0);
        if (study <= 0) {
            throw new IllegalArgumentException("Study must be > 0!");
        }
        samples = getConf().getInts("samples");
        if (samples == null || samples.length == 0) {
            throw new IllegalArgumentException("empty samples!");
        }

        genomeHelper = new GenomeHelper(getConf());
    }

    @Override
    protected void setupJob(Job job, String table) throws IOException {
        Scan scan = new Scan();
        int caching = job.getConfiguration().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 100);
        LOGGER.info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        for (int sample : samples) {
            byte[] column = VariantPhoenixHelper.buildSampleColumnKey(study, sample);
            scan.addColumn(genomeHelper.getColumnFamily(), column);
        }

        LOGGER.info("scan = " + scan.toJSON());

        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, table, outputTable, scan, SampleIndexerMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);
//        job.setCombinerClass(SampleIndexerCombiner.class);
        TableMapReduceUtil.initTableReducerJob(outputTable, SampleIndexerReducer.class, job);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Mutation.class);
        job.setNumReduceTasks(200);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new SampleIndexerDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + SampleIndexerDriver.class, e);
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


    public static class SampleIndexerMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

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

                PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(
                        cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength());

                String gt = array.getElement(0).toString();
                if (SampleIndexDBLoader.validGenotype(gt)) {
                    ImmutableBytesWritable key = new ImmutableBytesWritable(
                            SampleIndexConverter.toRowKey(sampleId, variant.getChromosome(), variant.getStart()));
                    ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(gt + ' ' + variant.toString()));
                    context.write(key, value);
                    if (samplesToCount.contains(sampleId)) {
                        context.getCounter("SAMPLE_INDEX", "SAMPLE_" + sampleId + '_' + gt).increment(1);
                    }
                }
            }
        }
    }

    public static class SampleIndexerReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        private byte[] family;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            family = new GenomeHelper(context.getConfiguration()).getColumnFamily();
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
                throws IOException, InterruptedException {
            Map<String, StringBuilder> gtsMap = new HashMap<>();
            for (ImmutableBytesWritable value : values) {
                String s = Bytes.toString(value.get());
                int idx = s.indexOf(' ');

                String gt = s.substring(0, idx);
                String variant = s.substring(idx + 1);
                gtsMap.computeIfAbsent(gt, k -> new StringBuilder()).append(variant).append(',');
            }
            Put put = new Put(key.get());
            for (Map.Entry<String, StringBuilder> entry : gtsMap.entrySet()) {
                put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }
            context.getCounter("SAMPLE_INDEX", "PUT").increment(1);

            context.write(key, put);
        }
    }

}
