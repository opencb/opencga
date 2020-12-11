package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey;

/**
 * Created by jacobo on 02/03/19.
 */
public class VariantHistogramDriver extends AbstractVariantsTableDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(VariantHistogramDriver.class);
    public static final String BATCH_SIZE = "batchSize";
    public static final String OUTPUT_DIR = "outputDir";
    private String region;
    private Integer batchSize;
    private String outputDir;

    @Override
    protected Class<VariantHistogramMapper> getMapperClass() {
        return VariantHistogramMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        HashMap<String, String> params = new HashMap<>();
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        params.put("--" + BATCH_SIZE, "<batch_size>");
        params.put("--" + OUTPUT_DIR, "<output_directory>*");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        region = getParam(VariantQueryParam.REGION.key());
        batchSize = Integer.valueOf(getParam(BATCH_SIZE, "1000000"));
        region = getParam(VariantQueryParam.REGION.key());
        outputDir = getParam(OUTPUT_DIR);
        if (StringUtils.isEmpty(outputDir)) {
            throw new IllegalArgumentException("Missing output directory.");
        }
        LOGGER.info("Write result to " + outputDir);
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan();

        for (Integer studyId : getMetadataManager().getStudyIds()) {
            scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStudyColumn(studyId).bytes());
        }
//        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.VariantColumn.TYPE.bytes());
        scan.setFilter(new FirstKeyOnlyFilter());

        if (StringUtils.isNotEmpty(region)) {
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());
        VariantMapReduceUtil.initTableMapperJob(job, variantTable, scan, getMapperClass(), VariantAlignedInputFormat.class);
        VariantMapReduceUtil.setNoneReduce(job);

        VariantAlignedInputFormat.setBatchSize(job, batchSize);
        VariantHistogramMapper.setBatchSize(job, batchSize);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "variants_histogram_" + batchSize;
    }


    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
//        if (succeed) {
//            Path path = new Path(outputDir);
//            path.getFileSystem(getConf()).copyToLocalFile(true, path, new Path());
//        }
    }

    public static class VariantHistogramMapper extends TableMapper<Text, IntWritable> {
        private static final String VARIANT_HISTOGRAM_BATCH_SIZE = "VariantHistogramMapper.batchSize";

        private Pair<String, Integer> currentBatch;
        private int count;
        private int batchSize;

        public static Job setBatchSize(Job job, int batchSize) {
            job.getConfiguration().setInt(VARIANT_HISTOGRAM_BATCH_SIZE, batchSize);
            return job;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            currentBatch = null;
            count = 0;
            batchSize = context.getConfiguration().getInt(VARIANT_HISTOGRAM_BATCH_SIZE, 10000);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Pair<String, Integer> pair = extractChrPosFromVariantRowKey(key.get(), key.getOffset(), key.getLength());
            pair.setSecond(pair.getSecond() / batchSize);
            if (!pair.equals(currentBatch)) {
                write(context);
                count = 0;
                currentBatch = pair;
            }
            count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            write(context);
        }

        protected void write(Context context) throws IOException, InterruptedException {
            if (currentBatch != null) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "batches").increment(1);
                String chr = currentBatch.getFirst();
                int pos = currentBatch.getSecond() * batchSize;
                context.write(
                        new Text(chr + ":" + pos + "-" + (pos + batchSize)),
                        new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new VariantHistogramDriver().privateMain(args));
        } catch (Exception e) {
            LOGGER.error("Error executing " + VariantHistogramDriver.class, e);
            System.exit(1);
        }
    }
}
