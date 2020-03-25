package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexPutBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAnnotationLoaderMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

    private static final String HAS_GENOTYPE = "SampleIndexAnnotationLoaderMapper.hasGenotype";
    private byte[] family;
    private Map<Integer, Map<String, AnnotationIndexPutBuilder>> annotationIndices = new HashMap<>();

    private boolean hasGenotype;
    private AnnotationIndexConverter converter;

    public static void setHasGenotype(Job job, boolean hasGenotype) {
        job.getConfiguration().setBoolean(HAS_GENOTYPE, hasGenotype);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        hasGenotype = context.getConfiguration().getBoolean(HAS_GENOTYPE, true);
        converter = new AnnotationIndexConverter();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        VariantRow variantRow = new VariantRow(result);
        AnnotationIndexEntry indexEntry = converter.convert(variantRow.getVariantAnnotation());
        // TODO Get stats given index values

        Set<String> samples = new HashSet<>(result.rawCells().length);

        variantRow.walker().onSample(sampleColumn -> {
            int sampleId = sampleColumn.getSampleId();
            String gt;
            boolean validGt;
            if (hasGenotype) {
                gt = sampleColumn.getGT();
                if (gt.isEmpty()) {
                    gt = GenotypeClass.NA_GT_VALUE;
                    validGt = true;
                } else {
                    validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                }
            } else {
                gt = GenotypeClass.NA_GT_VALUE;
                validGt = true;
            }
            // Avoid duplicates
            if (samples.add(sampleId + "_" + gt)) {
                if (validGt) {
                    annotationIndices
                            .computeIfAbsent(sampleId, k -> new HashMap<>())
                            .computeIfAbsent(gt, k -> new AnnotationIndexPutBuilder()).add(indexEntry);
                }
            }
        }).walk();
    }

    @Override
    public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {
        writeIndices(context, chromosome, position);
    }

    protected void writeIndices(Context context, String chromosome, int position) throws IOException, InterruptedException {
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "write_indices").increment(1);
        for (Map.Entry<Integer, Map<String, AnnotationIndexPutBuilder>> entry : annotationIndices.entrySet()) {
            Integer sampleId = entry.getKey();
            Put put = new Put(SampleIndexSchema.toRowKey(sampleId, chromosome, position));
            for (Map.Entry<String, AnnotationIndexPutBuilder> e : entry.getValue().entrySet()) {
                String gt = e.getKey();
                AnnotationIndexPutBuilder value = e.getValue();
                if (!value.isEmpty()) {
                    value.buildAndReset(put, gt, family);
                }
            }

            if (!put.isEmpty()) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "put").increment(1);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            } else {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "put_empty").increment(1);
            }
        }
    }
}
