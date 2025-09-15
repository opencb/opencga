package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class StreamVariantReducer extends Reducer<VariantLocusKey, Text, VariantLocusKey, Text> {

    public static final String STDOUT_KEY = "O:";
    public static final byte[] STDOUT_KEY_BYTES = Bytes.toBytes(STDOUT_KEY);
    public static final String STDERR_KEY = "E:";
    public static final byte[] STDERR_KEY_BYTES = Bytes.toBytes(STDERR_KEY);

    private static final Log LOG = LogFactory.getLog(StreamVariantReducer.class);
    public static final byte[] HEADER_PREFIX_BYTES = Bytes.toBytes("#");
    private MultipleOutputs<VariantLocusKey, Text> mos;
    private boolean headerWritten = false;

    @Override
    protected void setup(Reducer<VariantLocusKey, Text, VariantLocusKey, Text>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(VariantLocusKey key, Iterable<Text> values,
                          Reducer<VariantLocusKey, Text, VariantLocusKey, Text>.Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            if (key.getOther().startsWith(STDOUT_KEY)) {
                if (hasPrefix(value, HEADER_PREFIX_BYTES)) {
                    if (headerWritten) {
                        // skip header
                        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "header_records_skip").increment(1);
                    } else {
                        mos.write(StreamVariantDriver.STDOUT_NAMED_OUTPUT, key, value);
                        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "header_records").increment(1);
                    }
                } else {
                    // length < 3 to include lines with a small combination of \n \r \t and spaces.
                    if (value.getLength() < 3 && StringUtils.isBlank(value.toString())) {
                        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stdout_records_empty").increment(1);
                        // Do not interrupt header with empty records
                    } else {
                        // No more header, assume all header is written
                        headerWritten = true;
                    }
                    mos.write(StreamVariantDriver.STDOUT_NAMED_OUTPUT, key, value);
                    context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "body_records").increment(1);
                }
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stdout_records").increment(1);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stdout_records_bytes")
                        .increment(value.getLength());
            } else if (key.getOther().startsWith(STDERR_KEY)) {
                mos.write(StreamVariantDriver.STDERR_NAMED_OUTPUT, key, value);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stderr_records").increment(1);
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "stderr_records_bytes")
                        .increment(value.getLength());
            } else {
                throw new IllegalStateException("Unknown key " + key);
            }
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "records").increment(1);
        }
    }

    private static boolean hasPrefix(ImmutableBytesWritable key, byte[] prefix) {
        return hasPrefix(key.get(), key.getOffset(), key.getLength(), prefix);
    }

    private static boolean hasPrefix(Text text, byte[] prefix) {
        return hasPrefix(text.getBytes(), 0, text.getLength(), prefix);
    }

    private static boolean hasPrefix(byte[] key, int offset, int length, byte[] prefix) {
        if (length < prefix.length) {
            return false;
        }
        return Bytes.equals(key, offset, prefix.length, prefix, 0, prefix.length);
    }

    @Override
    protected void cleanup(Reducer<VariantLocusKey, Text, VariantLocusKey, Text>.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}
