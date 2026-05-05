package org.opencb.opencga.storage.hadoop.variant.index.sample.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.core.variant.index.core.IndexUtils;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexVariantAnnotationBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

public class SampleIndexVariantAnnotationPutBuilder extends SampleIndexVariantAnnotationBuilder {

    public SampleIndexVariantAnnotationPutBuilder(SampleIndexSchema indexSchema) {
        super(indexSchema);
    }

    public SampleIndexVariantAnnotationPutBuilder(SampleIndexSchema indexSchema, int size) {
        super(indexSchema, size);
    }

    public Put buildAndReset(Put put, String gt, byte[] family) {
        SampleIndexEntry.SampleIndexGtEntry entry = super.buildAndReset(gt);

        put.addColumn(family, SampleIndexSchema.toAnnotationIndexColumn(gt), entry.getAnnotationIndex());
        put.addColumn(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt),
                IndexUtils.countPerBitToBytes(entry.getAnnotationCounts()));

        if (entry.getConsequenceTypeIndex() != null && entry.getConsequenceTypeIndex().length > 0) {
            put.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt), entry.getConsequenceTypeIndex());
            put.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt), entry.getBiotypeIndex());
            put.addColumn(family, SampleIndexSchema.toAnnotationTranscriptFlagIndexColumn(gt), entry.getTranscriptFlagIndex());
            put.addColumn(family, SampleIndexSchema.toAnnotationCtBtTfIndexColumn(gt), entry.getCtBtTfIndex());
        }

        put.addColumn(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt), entry.getPopulationFrequencyIndex());

        if (entry.getClinicalIndex() != null && entry.getClinicalIndex().length > 0) {
            put.addColumn(family, SampleIndexSchema.toAnnotationClinicalIndexColumn(gt), entry.getClinicalIndex());
        }
        return put;
    }
}
