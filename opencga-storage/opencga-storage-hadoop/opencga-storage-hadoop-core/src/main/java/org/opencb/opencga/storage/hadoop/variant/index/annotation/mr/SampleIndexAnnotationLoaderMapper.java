package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexAnnotationLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAnnotationLoaderMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

    private static final String HAS_GENOTYPE = "SampleIndexAnnotationLoaderMapper.hasGenotype";
    private byte[] family;
    private GenomeHelper helper;
    private Map<Integer, Map<String, ByteArrayOutputStream>> annotationIndices = new HashMap<>();
    private boolean hasGenotype;

    public static void setHasGenotype(Job job, boolean hasGenotype) {
        job.getConfiguration().setBoolean(HAS_GENOTYPE, hasGenotype);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        helper = new GenomeHelper(context.getConfiguration());
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        hasGenotype = context.getConfiguration().getBoolean(HAS_GENOTYPE, true);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        HBaseToVariantAnnotationConverter annotationConverter = new HBaseToVariantAnnotationConverter(helper, 0);

        byte index = new AnnotationIndexConverter().convert(annotationConverter.convert(result));
                // TODO Get stats given index values

        for (Cell cell : result.rawCells()) {
            if (VariantPhoenixHelper.isSampleCell(cell)) {
                Integer sampleId = VariantPhoenixHelper.extractSampleId(
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), true);

                String gt;
                boolean validGt;
                if (hasGenotype) {
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                            cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueLength());
                    PhoenixHelper.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, null);
                    if (ptr.getLength() == 0) {
                        gt = GenotypeClass.NA_GT_VALUE;
                        validGt = true;
                    } else {
                        gt = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());
                        validGt = SampleIndexAnnotationLoader.isAnnotatedGenotype(gt);
                    }
                } else {
                    gt = GenotypeClass.NA_GT_VALUE;
                    validGt = true;
                }

                if (validGt) {
                    annotationIndices
                            .computeIfAbsent(sampleId, k -> new HashMap<>())
                            .computeIfAbsent(gt, k -> new ByteArrayOutputStream(50)).write(index);
                }

            }
        }

    }

    @Override
    public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {
        writeIndices(context, chromosome, position);
    }

    protected void writeIndices(Context context, String chromosome, int position) throws IOException, InterruptedException {

        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "write_indices").increment(1);
        for (Map.Entry<Integer, Map<String, ByteArrayOutputStream>> entry : annotationIndices.entrySet()) {
            Integer sampleId = entry.getKey();
            Put put = new Put(SampleIndexSchema.toRowKey(sampleId, chromosome, position));
            for (Map.Entry<String, ByteArrayOutputStream> e : entry.getValue().entrySet()) {
                String gt = e.getKey();
                ByteArrayOutputStream value = e.getValue();
                if (value.size() > 0) {
                    // Copy byte array, as the ByteArrayOutputStream will be reset and reused!
                    byte[] annotationIndex = value.toByteArray();
                    put.addColumn(family, SampleIndexSchema.toAnnotationIndexColumn(gt), annotationIndex);
                    put.addColumn(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt),
                            IndexUtils.countPerBitToBytes(IndexUtils.countPerBit(annotationIndex)));
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
