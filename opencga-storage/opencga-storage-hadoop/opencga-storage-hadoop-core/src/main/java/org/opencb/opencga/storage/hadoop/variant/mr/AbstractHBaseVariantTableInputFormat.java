package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.function.Function;

public abstract class AbstractHBaseVariantTableInputFormat<KEYOUT> extends TransformInputFormat<ImmutableBytesWritable, Result, KEYOUT> {

    public static final String USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT = "hbase_variant_table_input_format.use_sample_index_table_input_format";
    public static final String MULTI_SCANS = "hbase_variant_table_input_format.multi_scans";
    private Function<Result, KEYOUT> converter;

    @Override
    protected void init(Configuration configuration) throws IOException {
        if (configuration.getBoolean(MULTI_SCANS, false)) {
            MultiTableInputFormat tableInputFormat;
            if (configuration.getBoolean(USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, false)) {
                tableInputFormat = new MultiRegionSampleIndexTableInputFormat();
            } else {
                tableInputFormat = new MultiTableInputFormat();
            }
            tableInputFormat.setConf(configuration);
            inputFormat = tableInputFormat;
        } else {
            TableInputFormat tableInputFormat;
            if (configuration.getBoolean(USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, false)) {
                tableInputFormat = new SampleIndexTableInputFormat();
            } else {
                tableInputFormat = new TableInputFormat();
            }
            tableInputFormat.setConf(configuration);
            inputFormat = tableInputFormat;
        }
        converter = initConverter(configuration);
    }

    protected abstract Function<Result, KEYOUT> initConverter(Configuration configuration) throws IOException;

    @Override
    public RecordReader<ImmutableBytesWritable, KEYOUT> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (inputFormat == null) {
            init(context.getConfiguration());
        }
        RecordReader<ImmutableBytesWritable, Result> recordReader = inputFormat.createRecordReader(split, context);
        return new RecordReaderTransform<>(recordReader, converter);
    }
}
