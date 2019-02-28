package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableInputSplitter;

import java.io.IOException;
import java.util.List;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAlignedInputFormat extends InputFormat {

    private static final String DELEGATED_INPUT_FORMAT = "SampleIndexAlignedInputFormat.inputFormat";

    private boolean init = false;
    private InputFormat<?, ?> delegatedInputFormat;

    public static Job setDelegatedInputFormat(Job job, Class<? extends InputFormat> delegatedInputFormat) {
        job.getConfiguration().setClass(DELEGATED_INPUT_FORMAT, delegatedInputFormat, InputFormat.class);
        return job;
    }

    private void init(Configuration conf) throws IOException {
        if (!init) {
            Class<? extends InputFormat> clazz = conf.getClass(DELEGATED_INPUT_FORMAT, TableInputFormat.class, InputFormat.class);

            delegatedInputFormat = ReflectionUtils.newInstance(clazz, conf);

            init = true;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        init(context.getConfiguration());
        List<InputSplit> splits = delegatedInputFormat.getSplits(context);
        return VariantsTableInputSplitter.alignInputSplits(splits, SampleIndexDBLoader.BATCH_SIZE);
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        init(context.getConfiguration());
        return delegatedInputFormat.createRecordReader(split, context);
    }
}
