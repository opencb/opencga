package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.io.ByteArrayOutputStream;

public class AnnotationIndexPutBuilder {

    private final ByteArrayOutputStream annotation;
    private final ByteArrayOutputStream biotype;
    private final ByteArrayOutputStream ct;
    private final ByteArrayOutputStream popFreq;
    private byte partialPopFreq;
    private int partialPopFreqIdx;
    private int numVariants;

    public AnnotationIndexPutBuilder() {
        this.annotation = new ByteArrayOutputStream(50);
        this.biotype = new ByteArrayOutputStream(20);
        this.ct = new ByteArrayOutputStream(30);
        this.popFreq = new ByteArrayOutputStream(10);
        partialPopFreq = 0;
        partialPopFreqIdx = 0;
        numVariants = 0;
    }

    public AnnotationIndexPutBuilder add(AnnotationIndexEntry indexEntry) {
        numVariants++;
        annotation.write(indexEntry.getSummaryIndex());

        if (!indexEntry.isIntergenic()) {
            ct.write(Bytes.toBytes(indexEntry.getCtIndex()), 0, Short.BYTES);
            biotype.write(indexEntry.getBtIndex());
        }
        for (byte popFreqIndex : indexEntry.getPopFreqIndex()) {
            partialPopFreq |= ((0b11 & popFreqIndex) << (partialPopFreqIdx * AnnotationIndexConverter.POP_FREQ_SIZE));
            partialPopFreqIdx++;
            if (partialPopFreqIdx * AnnotationIndexConverter.POP_FREQ_SIZE == Byte.SIZE) {
                popFreq.write(partialPopFreq);
                partialPopFreqIdx = 0;
                partialPopFreq = 0;
            }
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

        if (ct.size() > 0) {
            put.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt), ct.toByteArray());
            put.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt), biotype.toByteArray());
        }
        if (partialPopFreqIdx != 0) {
            popFreq.write(partialPopFreq);
        }
        put.addColumn(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt), popFreq.toByteArray());
        reset();
        return put;
    }

    public void reset() {
        annotation.reset();
        biotype.reset();
        ct.reset();
        popFreq.reset();
        partialPopFreq = 0;
        partialPopFreqIdx = 0;
        numVariants = 0;
    }
}
