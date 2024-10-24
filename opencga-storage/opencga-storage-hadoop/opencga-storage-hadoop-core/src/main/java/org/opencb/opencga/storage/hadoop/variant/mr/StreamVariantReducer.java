package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StreamVariantReducer extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {
    private static final Log LOG = LogFactory.getLog(StreamVariantReducer.class);

    @Override
    protected void setup(Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(key, value);
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stdout_records").increment(1);
        }

    }

    @Override
    protected void cleanup(Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
