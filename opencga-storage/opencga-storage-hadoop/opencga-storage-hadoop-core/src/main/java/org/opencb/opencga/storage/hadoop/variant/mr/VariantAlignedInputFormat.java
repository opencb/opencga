package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by jacobo on 02/03/19.
 */
public class VariantAlignedInputFormat extends InputFormat {

    private static final String DELEGATED_INPUT_FORMAT = "VariantAlignedInputFormat.inputFormat";
    private static final String BATCH_SIZE = "VariantAlignedInputFormat.batchSize";

    private boolean init = false;
    private InputFormat<?, ?> delegatedInputFormat;
    private int batchSize;

    public static Job setDelegatedInputFormat(Job job, Class<? extends InputFormat> delegatedInputFormat) {
        job.getConfiguration().setClass(DELEGATED_INPUT_FORMAT, delegatedInputFormat, InputFormat.class);
        return job;
    }

    public static Job setBatchSize(Job job, int batchSize) {
        job.getConfiguration().setInt(BATCH_SIZE, batchSize);
        return job;
    }

    private void init(Configuration conf) throws IOException {
        if (!init) {
            Class<? extends InputFormat> clazz = conf.getClass(DELEGATED_INPUT_FORMAT, TableInputFormat.class, InputFormat.class);

            delegatedInputFormat = ReflectionUtils.newInstance(clazz, conf);

            batchSize = conf.getInt(BATCH_SIZE, SampleIndexDBLoader.BATCH_SIZE);

            init = true;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        init(context.getConfiguration());
        List<InputSplit> splits = delegatedInputFormat.getSplits(context);
        return VariantsTableInputSplitter.alignInputSplits(splits, getBatchSize());
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        init(context.getConfiguration());
        return delegatedInputFormat.createRecordReader(split, context);
    }

    protected int getBatchSize() {
        return batchSize;
    }
}

