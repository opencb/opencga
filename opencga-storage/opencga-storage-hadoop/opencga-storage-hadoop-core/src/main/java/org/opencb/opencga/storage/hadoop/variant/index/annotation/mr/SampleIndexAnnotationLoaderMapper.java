package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseToSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexAnnotationLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.extractChrPosFromVariantRowKey;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAnnotationLoaderMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private byte[] family;
    private GenomeHelper helper;
    private Map<Integer, Map<String, ByteArrayOutputStream>> annotationIndices = new HashMap<>();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        String chromosome = "";
        int position = 0;

        List<Pair<ImmutableBytesWritable, Result>> buffer = new ArrayList<>();

        try {
            while(context.nextKeyValue()) {
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
                            return HBaseToSampleIndexConverter.INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(v1, v2);
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
                                || (position / SampleIndexDBLoader.BATCH_SIZE) != (newPosition / SampleIndexDBLoader.BATCH_SIZE)) {
//                            System.err.println("Write indices from chr " + chromosome + ":" + (position / SampleIndexDBLoader.BATCH_SIZE)
//                                    +". New pair is " + newChromosome + ":" + (newPosition / SampleIndexDBLoader.BATCH_SIZE));
                            writeIndices(context, chromosome, position);
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

            writeIndices(context, chromosome, position);
        } finally {
            this.cleanup(context);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        helper = new GenomeHelper(context.getConfiguration());
        family = helper.getColumnFamily();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        HBaseToVariantAnnotationConverter annotationConverter = new HBaseToVariantAnnotationConverter(helper, 0);

        byte index = new AnnotationIndexConverter().convert(annotationConverter.convert(result));

        for (Cell cell : result.rawCells()) {
            if (VariantPhoenixHelper.isSampleCell(cell)) {
                Integer sampleId = VariantPhoenixHelper.extractSampleId(
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), true);
                ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                        cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength());
                PArrayDataType.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, null);
                String gt = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

                if (SampleIndexAnnotationLoader.isAnnotatedGenotype(gt)) {
                    annotationIndices
                            .computeIfAbsent(sampleId, k -> new HashMap<>())
                            .computeIfAbsent(gt, k-> new ByteArrayOutputStream(50)).write(index);
                }

            }
        }

    }

    protected void writeIndices(Context context, String chromosome, int position) throws IOException, InterruptedException {

        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "write_indices").increment(1);
        for (Map.Entry<Integer, Map<String, ByteArrayOutputStream>> entry : annotationIndices.entrySet()) {
            Integer sampleId = entry.getKey();
            Put put = new Put(HBaseToSampleIndexConverter.toRowKey(sampleId, chromosome, position));
            for (Map.Entry<String, ByteArrayOutputStream> e : entry.getValue().entrySet()) {
                String gt = e.getKey();
                ByteArrayOutputStream value = e.getValue();
                if (value.size() > 0) {
                    put.addColumn(family, HBaseToSampleIndexConverter.toAnnotationIndexColumn(gt), value.toByteArray());
                }
            }

            if (!put.isEmpty()) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "put").increment(1);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            } else {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "put_empty").increment(1);
            }
        }

        annotationIndices.values().forEach(map -> map.values().forEach(ByteArrayOutputStream::reset));
    }
}
