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
    private boolean hasTfIndex;
    private int tfIndex;
    private Combination ctBtCombination;
    private Combination ctTfCombination;
    private BitBuffer popFreqIndex;
    private boolean hasClinical;
    private BitBuffer clinicalIndex;


    public AnnotationIndexEntry() {
    }

    public AnnotationIndexEntry(AnnotationIndexEntry other) {
        this();
        this.hasSummaryIndex = other.hasSummaryIndex;
        this.summaryIndex = other.summaryIndex;
        this.intergenic = other.intergenic;
        this.hasCtIndex = other.hasCtIndex;
        this.ctIndex = other.ctIndex;
        this.hasBtIndex = other.hasBtIndex;
        this.btIndex = other.btIndex;
        this.hasTfIndex = other.hasTfIndex;
        this.tfIndex = other.tfIndex;
        this.ctBtCombination = new Combination(other.ctBtCombination);
        this.ctTfCombination = new Combination(other.ctTfCombination);
        this.popFreqIndex = other.popFreqIndex == null ? null : new BitBuffer(other.popFreqIndex);
        this.hasClinical = other.hasClinical;
        this.clinicalIndex = other.clinicalIndex == null ? null : new BitBuffer(other.clinicalIndex);
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, int ctIndex, int btIndex, int tfIndex,
            Combination ctBtCombination, Combination ctTfCombination, BitBuffer popFreqIndex,
            boolean hasClinical, BitBuffer clinicalIndex) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.hasCtIndex = !intergenic;
        this.btIndex = btIndex;
        this.hasBtIndex = !intergenic;
        this.tfIndex = tfIndex;
        this.hasTfIndex = !intergenic;
        this.ctBtCombination = ctBtCombination == null ? new Combination() : ctBtCombination;
        this.ctTfCombination = ctTfCombination == null ? new Combination() : ctTfCombination;
        this.hasClinical = hasClinical;
        this.clinicalIndex = clinicalIndex;
        this.popFreqIndex = popFreqIndex;
    }

    public static AnnotationIndexEntry empty(SampleIndexSchema schema) {
        return new AnnotationIndexEntry()
                .setPopFreqIndex(new BitBuffer(schema.getPopFreqIndex().getBitsLength()))
                .setCtBtCombination(new Combination())
                .setCtTfCombination(new Combination());
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

    public boolean hasTfIndex() {
        return hasTfIndex;
    }

    public AnnotationIndexEntry setHasTfIndex(boolean hasTfIndex) {
        this.hasTfIndex = hasTfIndex;
        return this;
    }

    public int getTfIndex() {
        return tfIndex;
    }

    public AnnotationIndexEntry setTfIndex(int tfIndex) {
        hasTfIndex = true;
        this.tfIndex = tfIndex;
        return this;
    }

    public Combination getCtBtCombination() {
        return ctBtCombination;
    }

    public AnnotationIndexEntry setCtBtCombination(Combination ctBtCombination) {
        this.ctBtCombination = ctBtCombination;
        return this;
    }

    public Combination getCtTfCombination() {
        return ctTfCombination;
    }

    public AnnotationIndexEntry setCtTfCombination(Combination ctTfCombination) {
        this.ctTfCombination = ctTfCombination;
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

    public void clear() {
        hasSummaryIndex = false;
        hasBtIndex = false;
        hasCtIndex = false;
        hasTfIndex = false;
        hasClinical = false;
        ctBtCombination.setNumA(0);
        ctBtCombination.setNumB(0);
        ctTfCombination.setNumA(0);
        ctTfCombination.setNumB(0);
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
