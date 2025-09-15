package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.io.IOException;

public class PhoenixVariantRowTableInputFormat
        extends TransformInputFormat<NullWritable, ExposedResultSetDBWritable, VariantRow> {

    @Override
    protected void init(JobContext context) throws IOException {
        // Ensure PhoenixDriver is registered
        if (PhoenixDriver.INSTANCE == null) {
            throw new IOException("Error registering PhoenixDriver");
        }
        PhoenixConfigurationUtil.setInputClass(context.getConfiguration(), ExposedResultSetDBWritable.class);
        inputFormat = new CustomPhoenixInputFormat<>();
    }

    @Override
    public RecordReader<NullWritable, VariantRow> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (inputFormat == null) {
            init(context);
        }
        RecordReader<NullWritable, ExposedResultSetDBWritable> recordReader = inputFormat.createRecordReader(split, context);

        return new RecordReaderTransform<>(recordReader, dbWritable -> new VariantRow(dbWritable.getResultSet()));
    }
}
