package org.opencb.opencga.storage.hadoop.variant.index.sample.family;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

public class FamilyIndexPutBuilder extends FamilyIndexBuilder {

    public FamilyIndexPutBuilder(int sampleId) {
        super(sampleId);
    }

    public Put buildAndReset(String chromosome, int position) {
        SampleIndexEntry entry = buildAndResetEntry(chromosome, position);
        if (entry == null) {
            return null;
        }
        byte[] row = SampleIndexSchema.toRowKey(sampleId, chromosome, position);
        Put put = new Put(row);

        for (SampleIndexEntry.SampleIndexGtEntry gtEntry : entry.getGts().values()) {
            String gt = gtEntry.getGt();
            if (gtEntry.getParentsIndexLength() > 0) {
                put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SampleIndexSchema.toParentsGTColumn(gt), gtEntry.getParentsIndex());
            }
        }

        if (entry.getMendelianVariantsLength() > 0) {
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SampleIndexSchema.toMendelianErrorColumn(),
                    entry.getMendelianVariantsValue());
        }

        if (put.isEmpty()) {
            return null;
        } else {
            return put;
        }
    }
}
