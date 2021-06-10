package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.core.io.bit.BitOutputStream;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.ByteArrayOutputStream;

public class AnnotationIndexPutBuilder {

    private final ByteArrayOutputStream annotation;
    private final BitOutputStream biotype;
    private final BitOutputStream ct;
    private final BitOutputStream ctBt;
    private final BitOutputStream popFreq;
    private final BitOutputStream clinical;
    private final SampleIndexSchema indexSchema = SampleIndexSchema.defaultSampleIndexSchema();
    private int numVariants;

    public AnnotationIndexPutBuilder() {
        this(50);
    }

    public AnnotationIndexPutBuilder(int size) {
        this.annotation = new ByteArrayOutputStream(size);
        this.biotype = new BitOutputStream(size / 4);
        this.ct = new BitOutputStream(size / 2);
        this.ctBt = new BitOutputStream(size / 4);
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
        }
        popFreq.write(indexEntry.getPopFreqIndex());

        ctBt.write(indexSchema.getCtBtIndex().getField().encode(indexEntry.getCtBtCombination()));
//        byte[] ctBtMatrix = ctBtCombination.getCtBtMatrix();
//        for (int i = 0; i < ctBtCombination.getNumCt(); i++) {
//            ctBt.write(ctBtMatrix[i], ctBtCombination.getNumBt());
//        }
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
        }
        put.addColumn(family, SampleIndexSchema.toAnnotationCtBtIndexColumn(gt), ctBt.toByteArray());

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
        ctBt.reset();
        popFreq.reset();
        clinical.reset();
        numVariants = 0;
    }
}
