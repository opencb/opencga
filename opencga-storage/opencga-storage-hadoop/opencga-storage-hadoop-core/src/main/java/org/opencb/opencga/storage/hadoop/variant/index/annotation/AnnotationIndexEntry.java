package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.CombinationIndexSchema.Combination;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

public class AnnotationIndexEntry {

    private boolean hasSummaryIndex;
    private byte summaryIndex;
    private boolean intergenic;
    private boolean hasCtIndex;
    // should be a long? a BitBuffer?
    private int ctIndex;
    private boolean hasBtIndex;
    // should be a long? a BitBuffer?
    private int btIndex;
    private Combination ctBtCombination;
    private BitBuffer popFreqIndex;
    private boolean hasClinical;
    private BitBuffer clinicalIndex;


    public AnnotationIndexEntry() {
    }

    public AnnotationIndexEntry(AnnotationIndexEntry annotationIndexEntry) {
        this(
                annotationIndexEntry.summaryIndex,
                annotationIndexEntry.intergenic,
                annotationIndexEntry.ctIndex,
                annotationIndexEntry.btIndex,
                new Combination(annotationIndexEntry.ctBtCombination),
                annotationIndexEntry.popFreqIndex == null ? null : new BitBuffer(annotationIndexEntry.popFreqIndex),
                annotationIndexEntry.hasClinical,
                annotationIndexEntry.clinicalIndex == null ? null : new BitBuffer(annotationIndexEntry.clinicalIndex)
        );
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, int ctIndex, int btIndex, Combination ctBtCombination, BitBuffer popFreqIndex,
            boolean hasClinical, BitBuffer clinicalIndex) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.btIndex = btIndex;
        this.ctBtCombination = ctBtCombination == null ? new Combination() : ctBtCombination;
        this.hasClinical = hasClinical;
        this.clinicalIndex = clinicalIndex;
        this.popFreqIndex = popFreqIndex;
    }

    public static AnnotationIndexEntry empty(SampleIndexSchema schema) {
        return new AnnotationIndexEntry()
                .setPopFreqIndex(new BitBuffer(schema.getPopFreqIndex().getBitsLength()))
                .setCtBtCombination(new Combination());
    }

    public boolean hasSummaryIndex() {
        return hasSummaryIndex;
    }

    public AnnotationIndexEntry setHasSummaryIndex(boolean hasSummaryIndex) {
        this.hasSummaryIndex = hasSummaryIndex;
        return this;
    }

    public byte getSummaryIndex() {
        return summaryIndex;
    }

    public AnnotationIndexEntry setSummaryIndex(byte summaryIndex) {
        this.hasSummaryIndex = true;
        this.summaryIndex = summaryIndex;
        return this;
    }

    public boolean isIntergenic() {
        return intergenic;
    }

    public AnnotationIndexEntry setIntergenic(boolean intergenic) {
        this.intergenic = intergenic;
        return this;
    }

    public boolean hasCtIndex() {
        return hasCtIndex;
    }

    public AnnotationIndexEntry setHasCtIndex(boolean hasCtIndex) {
        this.hasCtIndex = hasCtIndex;
        return this;
    }

    public int getCtIndex() {
        return ctIndex;
    }

    public AnnotationIndexEntry setCtIndex(int ctIndex) {
        hasCtIndex = true;
        this.ctIndex = ctIndex;
        return this;
    }

    public boolean hasBtIndex() {
        return hasBtIndex;
    }

    public AnnotationIndexEntry setHasBtIndex(boolean hasBtIndex) {
        this.hasBtIndex = hasBtIndex;
        return this;
    }

    public int getBtIndex() {
        return btIndex;
    }

    public AnnotationIndexEntry setBtIndex(int btIndex) {
        setHasBtIndex(true);
        this.btIndex = btIndex;
        return this;
    }

    public Combination getCtBtCombination() {
        return ctBtCombination;
    }

    public AnnotationIndexEntry setCtBtCombination(Combination ctBtCombination) {
        this.ctBtCombination = ctBtCombination;
        return this;
    }

    public BitBuffer getPopFreqIndex() {
        return popFreqIndex;
    }

    public AnnotationIndexEntry setPopFreqIndex(BitBuffer popFreqIndex) {
        this.popFreqIndex = popFreqIndex;
        return this;
    }

    public boolean hasClinical() {
        return hasClinical;
    }

    public AnnotationIndexEntry setHasClinical(boolean hasClinical) {
        this.hasClinical = hasClinical;
        return this;
    }

    public BitBuffer getClinicalIndex() {
        return clinicalIndex;
    }

    public AnnotationIndexEntry setClinicalIndex(BitBuffer clinicalIndex) {
        this.clinicalIndex = clinicalIndex;
        return this;
    }

    @Override
    public String toString() {
        return "AnnotationIndexEntry{"
                + "summaryIndex=" + IndexUtils.byteToString(summaryIndex)
                + ", intergenic=" + intergenic
                + (intergenic
                    ? ""
                    : (", ctIndex=" + IndexUtils.intToString(ctIndex) + ", btIndex=" + IndexUtils.intToString(btIndex)))
                + ", popFreqIndex=" + popFreqIndex
                + '}';
    }
}
