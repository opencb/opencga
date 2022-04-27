package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Pair;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey;

/**
 * Created on 05/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantTableSampleIndexOrderMapper<KEYOUT, VALOUT> extends TableMapper<KEYOUT, VALOUT> {

    private final Logger logger = LoggerFactory.getLogger(VariantTableSampleIndexOrderMapper.class);

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        String chromosome = "";
        int position = 0;

        List<Pair<ImmutableBytesWritable, Result>> buffer = new ArrayList<>();

        try {
            while (context.nextKeyValue()) {
                ImmutableBytesWritable key = context.getCurrentKey();
                Result value = context.getCurrentValue();

                Pair<String, Integer> chrPosPair = extractChrPosFromVariantRowKey(key.get(), key.getOffset(), key.getLength());

                String newChromosome = chrPosPair.getFirst();
                Integer newPosition = chrPosPair.getSecond();
                // Group results, as long as they start in the same position
                if (newChromosome.equals(chromosome) && newPosition == position) {
                    buffer.add(new Pair<>(new ImmutableBytesWritable(key), value));
                } else {
                    // Current result does not start in the previous position.
                    // Sort buffer
                    if (buffer.size() > 1) {
                        buffer.sort((o1, o2) -> {
                            Variant v1 = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(o1.getSecond().getRow());
                            Variant v2 = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(o2.getSecond().getRow());
                            return SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(v1, v2);
                        });
                    }

                    if (!buffer.isEmpty()) {
                        // Drain buffer.
                        countBufferSize(context, buffer.size(), chromosome, position);
                        for (Pair<ImmutableBytesWritable, Result> pair : buffer) {
                            this.map(pair.getFirst(), pair.getSecond(), context);
                        }

                        buffer.clear();

                        // In event of new chromosome, or new batch, write indices
                        if (!newChromosome.equals(chromosome)
                                || (position / SampleIndexSchema.BATCH_SIZE) != (newPosition / SampleIndexSchema.BATCH_SIZE)) {
                            flush(context, chromosome, position);
                        }
                    }

                    // Store current variant.
                    chromosome = newChromosome;
                    position = newPosition;

                    buffer.add(new Pair<>(new ImmutableBytesWritable(key), value));
                }
            }
            countBufferSize(context, buffer.size(), chromosome, position);
            for (Pair<ImmutableBytesWritable, Result> pair : buffer) {
                this.map(pair.getFirst(), pair.getSecond(), context);
            }

            flush(context, chromosome, position);
        } finally {
            this.cleanup(context);
        }
    }

    private void countBufferSize(Context context, int size, String chromosome, int position) {
        String counterName;
        if (size < 5) {
            counterName = "buffer_size_" + size;
        } else if (size < 10) {
            counterName = "buffer_size_5-10";
        } else if (size < 20) {
            counterName = "buffer_size_10-20";
        } else if (size < 100) {
            counterName = "buffer_size_20-100";
        } else {
            logger.warn("Super large buffer size of {} at {}:{}", size, chromosome, position);
            if (size < 1000) {
                counterName = "buffer_size_100-1000";
            } else {
                counterName = "buffer_size_gt1000";
            }
        }
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, counterName).increment(1);
    }

    protected abstract void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException;


    public abstract void flush(Context context, String chromosome, int position) throws IOException, InterruptedException;

}
