package org.opencb.opencga.storage.hadoop.variant.index.sample.file;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeSet;

import static org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema.toRowKey;

public class SampleIndexEntryPutBuilder extends SampleIndexEntryBuilder {
    private static final byte[] COLUMN_FAMILY = GenomeHelper.COLUMN_FAMILY_BYTES;

    public SampleIndexEntryPutBuilder(int sampleId, Variant variant, SampleIndexSchema schema, boolean orderedInput,
                                      boolean multiFileSample) {
        super(sampleId, variant, schema, orderedInput, multiFileSample);
    }

    public SampleIndexEntryPutBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema, boolean orderedInput,
                                      boolean multiFileSample) {
        super(sampleId, chromosome, position, schema, orderedInput, multiFileSample);
    }

    public SampleIndexEntryPutBuilder(int sampleId, String chromosome, int position, SampleIndexSchema schema,
                                      Map<String, TreeSet<SampleIndexVariant>> map) {
        super(sampleId, chromosome, position, schema, map);
    }

    public Put build() {
        return build(this.buildEntry());
    }

    public static Put build(SampleIndexEntry entry) {
        byte[] rk = toRowKey(entry.getSampleId(), entry.getChromosome(), entry.getBatchStart());
        Put put = new Put(rk);
        if (entry.getGts().isEmpty()) {
            return put;
        }

        for (SampleIndexEntry.SampleIndexGtEntry gtEntry : entry.getGts().values()) {
            String gt = gtEntry.getGt();
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeColumn(gt), gtEntry.getVariants());
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeCountColumn(gt), Bytes.toBytes(gtEntry.getCount()));
            put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toFileIndexColumn(gt), gtEntry.getFileIndex());
            if (gtEntry.getFileDataLength() > 0) {
                put.addColumn(COLUMN_FAMILY, ByteBuffer.wrap(SampleIndexSchema.toFileDataColumn(gt)),
                        put.getTimestamp(), gtEntry.getFileDataIndexBuffer());
            }
        }
        put.addColumn(COLUMN_FAMILY, SampleIndexSchema.toGenotypeDiscrepanciesCountColumn(), Bytes.toBytes(entry.getDiscrepancies()));

        return put;
    }

}
