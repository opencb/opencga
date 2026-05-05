package org.opencb.opencga.storage.core.variant.index.sample.annotation;

import org.opencb.opencga.storage.core.io.bit.BitOutputStream;
import org.opencb.opencga.storage.core.variant.index.core.IndexUtils;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariantAnnotation;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.ByteArrayOutputStream;

public class SampleIndexVariantAnnotationBuilder {

    private final ByteArrayOutputStream annotation;
    private final BitOutputStream biotype;
    private final BitOutputStream transcriptFlag;
    private final BitOutputStream ct;
    private final BitOutputStream ctBtTf;
    private final BitOutputStream popFreq;
    private final BitOutputStream clinical;
    private final SampleIndexSchema indexSchema;
    private int numVariants;

    public SampleIndexVariantAnnotationBuilder(SampleIndexSchema indexSchema) {
        this(indexSchema, 50);
    }

    public SampleIndexVariantAnnotationBuilder(SampleIndexSchema indexSchema, int size) {
        this.indexSchema = indexSchema;
        this.annotation = new ByteArrayOutputStream(size);
        this.biotype = new BitOutputStream(size / 4);
        this.ct = new BitOutputStream(size / 2);
        this.transcriptFlag = new BitOutputStream(size / 2);
        this.ctBtTf = new BitOutputStream(size / 4);
        this.popFreq = new BitOutputStream(size / 2);
        this.clinical = new BitOutputStream(size / 5);
        numVariants = 0;
    }

    public SampleIndexVariantAnnotationBuilder add(SampleIndexVariantAnnotation indexEntry) {
        numVariants++;
        annotation.write(indexEntry.getSummaryIndex());

        if (!indexEntry.isIntergenic()) {
            ct.write(indexEntry.getCtIndex(), indexSchema.getCtIndex().getBitsLength());
            biotype.write(indexEntry.getBtIndex(), indexSchema.getBiotypeIndex().getBitsLength());
            transcriptFlag.write(indexEntry.getTfIndex(), indexSchema.getTranscriptFlagIndexSchema().getBitsLength());
            ctBtTf.write(indexSchema.getCtBtTfIndex().getField().encode(indexEntry.getCtBtTfCombination()));
        }
        popFreq.write(indexEntry.getPopFreqIndex());

        if (indexEntry.hasClinical()) {
            clinical.write(indexEntry.getClinicalIndex());
        }
        return this;
    }

    public boolean isEmpty() {
        return numVariants == 0;
    }

    public SampleIndexEntry.SampleIndexGtEntry buildAndReset(String gt) {
        // Copy byte array, as the ByteArrayOutputStream could be reset and reused!
        SampleIndexEntry.SampleIndexGtEntry entry = new SampleIndexEntry.SampleIndexGtEntry(gt);
        byte[] annotationIndex = annotation.toByteArray();
        entry.setAnnotationIndex(annotationIndex);
        entry.setAnnotationCounts(IndexUtils.countPerBit(annotationIndex));

        if (!ct.isEmpty()) {
            entry.setConsequenceTypeIndex(ct.toByteArray());
            entry.setBiotypeIndex(biotype.toByteArray());
            entry.setTranscriptFlagIndex(transcriptFlag.toByteArray());
            entry.setCtBtTfIndex(ctBtTf.toByteArray());
        }
        entry.setPopulationFrequencyIndex(popFreq.toByteArray());

        if (clinical.getBitLength() > 0) {
            entry.setClinicalIndex(clinical.toByteArray());
        }
        reset();
        return entry;
    }

    public void buildAndReset(SampleIndexEntry.SampleIndexGtEntry gtEntry) {
        // Update the gtEntry with new annotation indices
        byte[] annotationIndex = annotation.toByteArray();
        gtEntry.setAnnotationIndex(annotationIndex);
        gtEntry.setAnnotationCounts(IndexUtils.countPerBit(annotationIndex));

        if (!ct.isEmpty()) {
            gtEntry.setConsequenceTypeIndex(ct.toByteArray());
            gtEntry.setBiotypeIndex(biotype.toByteArray());
            gtEntry.setTranscriptFlagIndex(transcriptFlag.toByteArray());
            gtEntry.setCtBtTfIndex(ctBtTf.toByteArray());
        }
        gtEntry.setPopulationFrequencyIndex(popFreq.toByteArray());

        if (clinical.getBitLength() > 0) {
            gtEntry.setClinicalIndex(clinical.toByteArray());
        }
        reset();
    }

    public void reset() {
        annotation.reset();
        biotype.reset();
        ct.reset();
        transcriptFlag.reset();
        ctBtTf.reset();
        popFreq.reset();
        clinical.reset();
        numVariants = 0;
    }
}
