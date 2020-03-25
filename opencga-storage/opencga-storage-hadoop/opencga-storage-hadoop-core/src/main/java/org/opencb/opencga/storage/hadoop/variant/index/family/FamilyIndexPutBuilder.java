package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FamilyIndexPutBuilder {

    private final int sampleId;

    private ByteArrayOutputStream mendelianErrors;
    private Map<String, ByteArrayOutputStream> parentsGTMap;

    public FamilyIndexPutBuilder(int sampleId) {
        this.sampleId = sampleId;
        mendelianErrors = new ByteArrayOutputStream();
        parentsGTMap = new HashMap<>();
    }

    public void addParents(String childGtStr, String fatherGtStr, String motherGtStr) {
        if (SampleIndexSchema.validGenotype(childGtStr)) {
            parentsGTMap.computeIfAbsent(childGtStr, gt -> new ByteArrayOutputStream())
                    .write(GenotypeCodec.encode(fatherGtStr, motherGtStr));
        }
    }

    public void addParents(String childGtStr, Collection<String> fatherGtStr, Collection<String> motherGtStr) {
        if (SampleIndexSchema.validGenotype(childGtStr)) {
            parentsGTMap.computeIfAbsent(childGtStr, gt -> new ByteArrayOutputStream())
                    .write(GenotypeCodec.encode(fatherGtStr, motherGtStr));
        }
    }

    public void addMendelianError(Variant variant, String childGtStr, int idx, Integer me) throws IOException {
        if (me > 0) {
            MendelianErrorSampleIndexConverter.toBytes(mendelianErrors, variant, childGtStr, idx, me);
        }
    }

    public Put buildAndReset(String chromosome, int position) {
        byte[] row = SampleIndexSchema.toRowKey(sampleId, chromosome, position);
        Put put = new Put(row);

        for (Map.Entry<String, ByteArrayOutputStream> gtEntry : parentsGTMap.entrySet()) {
            String gt = gtEntry.getKey();
            ByteArrayOutputStream gtValue = gtEntry.getValue();

            if (gtValue.size() > 0) {
                // Copy value, as the ByteArrayOutputStream is erased
                byte[] value = gtValue.toByteArray();
                put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SampleIndexSchema.toParentsGTColumn(gt), value);
                gtValue.reset();
            }
        }

        if (mendelianErrors.size() > 0) {
            // Copy value, as the ByteArrayOutputStream is erased
            byte[] value = mendelianErrors.toByteArray();
            put.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, SampleIndexSchema.toMendelianErrorColumn(), value);
            mendelianErrors.reset();
        }

        if (put.isEmpty()) {
            return null;
        } else {
            return put;
        }
    }

    public byte[] getMendelianErrors() {
        return mendelianErrors.toByteArray();
    }
}
