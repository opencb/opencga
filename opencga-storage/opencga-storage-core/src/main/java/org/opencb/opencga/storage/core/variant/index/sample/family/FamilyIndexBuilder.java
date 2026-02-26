package org.opencb.opencga.storage.core.variant.index.sample.family;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.codecs.GenotypeCodec;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FamilyIndexBuilder {

    protected final int sampleId;

    private ByteArrayOutputStream mendelianErrors;
    private Map<String, ByteArrayOutputStream> parentsGTMap;

    public FamilyIndexBuilder(int sampleId) {
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

    public SampleIndexEntry buildAndResetEntry(String chromosome, int position) {
        SampleIndexEntry entry = new SampleIndexEntry(sampleId, chromosome, position);
        boolean empty = true;

        for (Map.Entry<String, ByteArrayOutputStream> gtEntry : parentsGTMap.entrySet()) {
            String gt = gtEntry.getKey();
            ByteArrayOutputStream gtValue = gtEntry.getValue();

            if (gtValue.size() > 0) {
                // Copy value, as the ByteArrayOutputStream is erased
                byte[] value = gtValue.toByteArray();
                entry.getGtEntry(gt).setParentsIndex(value);
                gtValue.reset();
                empty = false;
            }
        }

        if (mendelianErrors.size() > 0) {
            // Copy value, as the ByteArrayOutputStream is erased
            byte[] value = mendelianErrors.toByteArray();
            entry.setMendelianVariants(value);
            mendelianErrors.reset();
            empty = false;
        }

        if (empty) {
            return null;
        } else {
            return entry;
        }
    }

    public byte[] getMendelianErrors() {
        return mendelianErrors.toByteArray();
    }
}
