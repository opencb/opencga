package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexPutBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.IOException;
import java.util.*;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAnnotationLoaderMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

    private static final String HAS_GENOTYPE = "SampleIndexAnnotationLoaderMapper.hasGenotype";
    private static final String MULTI_FILE_SAMPLES = "SampleIndexAnnotationLoaderMapper.multiFileSamples";
    private static final String FIRST_SAMPLE_ID = "SampleIndexAnnotationLoaderMapper.firstSampleId";
    private static final String LAST_SAMPLE_ID = "SampleIndexAnnotationLoaderMapper.lastSampleId";
    private byte[] family;
    private Map<String, AnnotationIndexPutBuilder>[] annotationIndices;

    private boolean hasGenotype;
    private boolean multiFileSamples;
    private AnnotationIndexConverter converter;
    private int firstSampleId;
    private SampleIndexSchema schema;

    public static void setHasGenotype(Job job, boolean hasGenotype) {
        job.getConfiguration().setBoolean(HAS_GENOTYPE, hasGenotype);
    }

    public static void setMultiFileSamples(Job job, boolean multiFileSamples) {
        job.getConfiguration().setBoolean(MULTI_FILE_SAMPLES, multiFileSamples);
    }

    public static void setSampleIdRange(Job job, Collection<Integer> sampleIds) {
        int start = sampleIds.stream().mapToInt(Integer::intValue).min().orElse(0);
        int end = sampleIds.stream().mapToInt(Integer::intValue).max().orElse(0);
        job.getConfiguration().setInt(FIRST_SAMPLE_ID, start);
        job.getConfiguration().setInt(LAST_SAMPLE_ID, end);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        hasGenotype = context.getConfiguration().getBoolean(HAS_GENOTYPE, true);
        multiFileSamples = context.getConfiguration().getBoolean(MULTI_FILE_SAMPLES, false);
        firstSampleId = context.getConfiguration().getInt(FIRST_SAMPLE_ID, 0);
        int lastSampleId = context.getConfiguration().getInt(LAST_SAMPLE_ID, 0);
        annotationIndices = new Map[lastSampleId - firstSampleId + 1];
        for (int i = 0; i < annotationIndices.length; i++) {
            annotationIndices[i] = new HashMap<>();
        }
        schema = VariantMapReduceUtil.getSampleIndexSchema(context.getConfiguration());
        converter = new AnnotationIndexConverter(schema);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        VariantRow variantRow = new VariantRow(result);
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variants").increment(1);
        VariantAnnotation variantAnnotation = variantRow.getVariantAnnotation();
        if (variantAnnotation == null) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "variantsAnnotationNull").increment(1);
        }
        AnnotationIndexEntry indexEntry = converter.convert(variantAnnotation);
        // TODO Get stats given index values

        Set<String> samples = multiFileSamples ? new HashSet<>(result.rawCells().length) : null;

        variantRow.walker().onSample(sampleColumn -> {
            int sampleId = sampleColumn.getSampleId();
            String gt;
            boolean validGt;
            if (hasGenotype) {
                gt = sampleColumn.getGT();
                if (gt == null || gt.isEmpty()) {
                    gt = GenotypeClass.NA_GT_VALUE;
                    validGt = true;
                } else {
                    validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                }
            } else {
                gt = GenotypeClass.NA_GT_VALUE;
                validGt = true;
            }
            // Avoid duplicates on multiFileSamples
            if (samples == null || samples.add(sampleId + "_" + gt)) {
                if (validGt) {
                    annotationIndices[sampleId - firstSampleId]
                            .computeIfAbsent(gt, k -> new AnnotationIndexPutBuilder(schema)).add(indexEntry);
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
        for (int i = 0; i < annotationIndices.length; i++) {
            Map<String, AnnotationIndexPutBuilder> gtMap = annotationIndices[i];
            int sampleId = i + firstSampleId;
            Put put = new Put(SampleIndexSchema.toRowKey(sampleId, chromosome, position));
            for (Map.Entry<String, AnnotationIndexPutBuilder> e : gtMap.entrySet()) {
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
