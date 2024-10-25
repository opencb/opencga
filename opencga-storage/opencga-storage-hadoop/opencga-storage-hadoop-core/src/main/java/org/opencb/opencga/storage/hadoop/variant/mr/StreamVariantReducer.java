package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class StreamVariantReducer extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {

    public static final String STDOUT_KEY = "O:";
    public static final byte[] STDOUT_KEY_BYTES = Bytes.toBytes(STDOUT_KEY);
    public static final String STDERR_KEY = "E:";
    public static final byte[] STDERR_KEY_BYTES = Bytes.toBytes(STDERR_KEY);

    private static final Log LOG = LogFactory.getLog(StreamVariantReducer.class);
    private MultipleOutputs<ImmutableBytesWritable, Text> mos;

    @Override
    protected void setup(Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            if (Bytes.equals(key.get(), key.getOffset(), STDOUT_KEY_BYTES.length, STDOUT_KEY_BYTES, 0, STDOUT_KEY_BYTES.length)) {
                mos.write("stdout", key, value);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stdout_records").increment(1);
            } else {
                mos.write("stderr", key, value);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stderr_records").increment(1);
            }
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "records").increment(1);
        }
    }

    @Override
    protected void cleanup(Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}
