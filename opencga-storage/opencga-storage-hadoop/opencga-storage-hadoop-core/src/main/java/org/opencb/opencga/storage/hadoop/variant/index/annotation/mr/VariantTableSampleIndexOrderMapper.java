package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Pair;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

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
                        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME,
                                "buffer_size_" + ((buffer.size() > 5) ? "X" : (buffer.size()))).increment(1);
                        for (Pair<ImmutableBytesWritable, Result> pair : buffer) {
                            this.map(pair.getFirst(), pair.getSecond(), context);
                        }

                        buffer.clear();

                        // In event of new chromosome, or new batch, write indices
                        if (!newChromosome.equals(chromosome)
                                || (position / SampleIndexSchema.BATCH_SIZE) != (newPosition / SampleIndexSchema.BATCH_SIZE)) {
//                            System.err.println("Write indices from chr " + chromosome + ":" + (position / SampleIndexDBLoader.BATCH_SIZE)
//                                    +". New pair is " + newChromosome + ":" + (newPosition / SampleIndexDBLoader.BATCH_SIZE));
                            flush(context, chromosome, position);
                        }
                    }

                    // Store current variant.
                    chromosome = newChromosome;
                    position = newPosition;

                    buffer.add(new Pair<>(new ImmutableBytesWritable(key), value));
                }
            }
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME,
                    "buffer_size_" + ((buffer.size() > 5) ? "gt5" : (buffer.size()))).increment(1);
            for (Pair<ImmutableBytesWritable, Result> pair : buffer) {
                this.map(pair.getFirst(), pair.getSecond(), context);
            }

            flush(context, chromosome, position);
        } finally {
            this.cleanup(context);
        }
    }

    public abstract void flush(Context context, String chromosome, int position) throws IOException, InterruptedException;

}
