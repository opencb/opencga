package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

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

            batchSize = conf.getInt(BATCH_SIZE, SampleIndexSchema.BATCH_SIZE);

            init = true;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        init(context.getConfiguration());
        List<InputSplit> splits;
        if (delegatedInputFormat instanceof TableInputFormatBase) {
            // If the table is a TableInputFormatBase, set a lighter version of the scan, as it does not need the whole object.
            // Prevent a massive memory usage with large scans over huge tables.
            // The current implementation (hdp 3.1, hbase 2.0.2) includes a serialized copy of the scan on each TableSplit.
            // From this TableSplit.scan, only the startRow and the stopRow are read.
            TableInputFormat tableInputFormat = (TableInputFormat) delegatedInputFormat;
            Scan scan = tableInputFormat.getScan();
            tableInputFormat.setScan(new Scan()
                    .setStartRow(scan.getStartRow())
                    .setStopRow(scan.getStopRow()));
            splits = tableInputFormat.getSplits(context);
            tableInputFormat.setScan(scan);
        } else {
            splits = delegatedInputFormat.getSplits(context);
        }

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

