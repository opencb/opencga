package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.core.io.bit.BitOutputStream;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.ByteArrayOutputStream;

public class AnnotationIndexPutBuilder {

    private final ByteArrayOutputStream annotation;
    private final BitOutputStream biotype;
    private final BitOutputStream transcriptFlag;
    private final BitOutputStream ct;
    private final BitOutputStream ctBtTf;
    private final BitOutputStream popFreq;
    private final BitOutputStream clinical;
    private final SampleIndexSchema indexSchema;
    private int numVariants;

    public AnnotationIndexPutBuilder(SampleIndexSchema indexSchema) {
        this(indexSchema, 50);
    }

    public AnnotationIndexPutBuilder(SampleIndexSchema indexSchema, int size) {
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

    public AnnotationIndexPutBuilder add(AnnotationIndexEntry indexEntry) {
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

    public Put buildAndReset(Put put, String gt, byte[] family) {
        // Copy byte array, as the ByteArrayOutputStream could be reset and reused!
        byte[] annotationIndex = annotation.toByteArray();
        put.addColumn(family, SampleIndexSchema.toAnnotationIndexColumn(gt), annotationIndex);
        put.addColumn(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt),
                IndexUtils.countPerBitToBytes(IndexUtils.countPerBit(annotationIndex)));

        if (!ct.isEmpty()) {
            put.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt), ct.toByteArray());
            put.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt), biotype.toByteArray());
            put.addColumn(family, SampleIndexSchema.toAnnotationTranscriptFlagIndexColumn(gt), transcriptFlag.toByteArray());
            put.addColumn(family, SampleIndexSchema.toAnnotationCtBtTfIndexColumn(gt), ctBtTf.toByteArray());
        }

        put.addColumn(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt), popFreq.toByteArray());

        if (clinical.getBitLength() > 0) {
            put.addColumn(family, SampleIndexSchema.toAnnotationClinicalIndexColumn(gt), clinical.toByteArray());
        }
        reset();
        return put;
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
