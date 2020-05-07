package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

public class AnnotationIndexEntry {

    private boolean hasSummaryIndex;
    private byte summaryIndex;
    private boolean intergenic;
    private boolean hasCtIndex;
    private short ctIndex;
    private boolean hasBtIndex;
    private byte btIndex;
    private CtBtCombination ctBtCombination;
    private boolean clinical;
    private byte[] popFreqIndex;
    private byte clinicalIndex;


    public AnnotationIndexEntry() {
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, short ctIndex, byte btIndex, byte[] ctBtCombination, byte[] popFreqIndex,
            boolean clinical, byte clinicalIndex) {
        this(summaryIndex, intergenic, ctIndex, btIndex, new CtBtCombination(ctBtCombination,
                Integer.bitCount(Short.toUnsignedInt(ctIndex)),
                Integer.bitCount(Byte.toUnsignedInt(btIndex))), popFreqIndex,
                clinical, clinicalIndex);
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, short ctIndex, byte btIndex, CtBtCombination ctBtCombination, byte[] popFreqIndex,
            boolean clinical, byte clinicalIndex) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.btIndex = btIndex;
        this.ctBtCombination = ctBtCombination == null ? CtBtCombination.empty() : ctBtCombination;
        this.clinical = clinical;
        this.clinicalIndex = clinicalIndex;
        this.popFreqIndex = popFreqIndex;
    }

    /**
     * Matrix that contains the ConsequenceType and Biotype transcript combinations in one variant.
     *
     * Each non-intergenic variant has a set of pairs (CT, BT), which defines a list of CT and BT values.
     * This can be represented as a matrix where the rows are CT values in the variant, the columns are BT values,
     * and the intersection is a boolean representing if that specific combination occurs in the variant.
     *
     *       +---------+---------+----+
     *       | bt1     | bt2     | ...|
     * +-----+---------+---------+----+
     * | ct1 | ct1-bt1 | ct1-bt2 |    |
     * | ct2 | ct2+bt1 | ct2+bt2 |    |
     * | ... |         |         | ...|
     * +-----+---------+---------+----+

     * In this class, the matrix is stored as an array of rows: ctBtMatrix = {ct1_row, ct2_row, ...}
     * As the max number of BTs is 8, we can use an array of bytes.
     * This matrix can be stored sequentially using {@link org.opencb.opencga.storage.core.io.bit.BitOutputStream}.
     *
     * Then, to read the matrix, first has to determine its size, by counting the number of unique CT and BT
     * in the index (see {@link Integer#bitCount}).
     * The order of the rows and columns matches with the order of the bits within the index.
     *
     */
    public static class CtBtCombination {
        public static final CtBtCombination EMPTY = new CtBtCombination(new byte[0], 0, 0);
        private byte[] ctBtMatrix;
        private int numCt;
        private int numBt;

        public CtBtCombination(byte[] ctBtMatrix, int numCt, int numBt) {
            this.ctBtMatrix = ctBtMatrix;
            this.numCt = numCt;
            this.numBt = numBt;
        }

        public byte[] getCtBtMatrix() {
            return ctBtMatrix;
        }

        public CtBtCombination setCtBtMatrix(byte[] ctBtMatrix) {
            this.ctBtMatrix = ctBtMatrix;
            return this;
        }

        public int getNumCt() {
            return numCt;
        }

        public CtBtCombination setNumCt(int numCt) {
            this.numCt = numCt;
            return this;
        }

        public int getNumBt() {
            return numBt;
        }

        public CtBtCombination setNumBt(int numBt) {
            this.numBt = numBt;
            return this;
        }

        public static CtBtCombination empty() {
            return EMPTY;
        }
    }

    public static AnnotationIndexEntry empty(int numPopFreq) {
        return new AnnotationIndexEntry().setPopFreqIndex(new byte[numPopFreq]).setCtBtCombination(new CtBtCombination(new byte[0], 0, 0));
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

    public short getCtIndex() {
        return ctIndex;
    }

    public AnnotationIndexEntry setCtIndex(short ctIndex) {
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

    public byte getBtIndex() {
        return btIndex;
    }

    public AnnotationIndexEntry setBtIndex(byte btIndex) {
        setHasBtIndex(true);
        this.btIndex = btIndex;
        return this;
    }

    public CtBtCombination getCtBtCombination() {
        return ctBtCombination;
    }

    public AnnotationIndexEntry setCtBtCombination(CtBtCombination ctBtCombination) {
        this.ctBtCombination = ctBtCombination;
        return this;
    }

    public byte[] getPopFreqIndex() {
        return popFreqIndex;
    }

    public AnnotationIndexEntry setPopFreqIndex(byte[] popFreqIndex) {
        this.popFreqIndex = popFreqIndex;
        return this;
    }

    public boolean isClinical() {
        return clinical;
    }

    public AnnotationIndexEntry setClinical(boolean clinical) {
        this.clinical = clinical;
        return this;
    }

    public byte getClinicalIndex() {
        return clinicalIndex;
    }

    public AnnotationIndexEntry setClinicalIndex(byte clinicalIndex) {
        this.clinicalIndex = clinicalIndex;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder pf = new StringBuilder("[");
        for (byte freqIndex : popFreqIndex) {
            pf.append(IndexUtils.byteToString(freqIndex)).append(", ");
        }
        pf.append("]");
        return "AnnotationIndexEntry{"
                + "summaryIndex=" + IndexUtils.byteToString(summaryIndex)
                + ", intergenic=" + intergenic
                + (intergenic
                    ? ""
                    : (", ctIndex=" + IndexUtils.shortToString(ctIndex) + ", btIndex=" + IndexUtils.byteToString(btIndex)))
                + ", popFreqIndex=" + pf
                + '}';
    }
}
