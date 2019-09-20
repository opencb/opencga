package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

public class AnnotationIndexEntry {

    public static final byte MISSING_ANNOTATION = (byte) 0xFF;
    private final byte summaryIndex;
    private final boolean intergenic;
    private final short ctIndex;
    private final byte btIndex;
    private final CtBtCombination ctBtCombination;
    private final byte[] popFreqIndex;


    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, short ctIndex, byte btIndex, byte[] popFreqIndex, byte[] ctBtCombination) {
        this(summaryIndex, intergenic, ctIndex, btIndex, popFreqIndex,
                new CtBtCombination(ctBtCombination,
                        Integer.bitCount(Short.toUnsignedInt(ctIndex)),
                        Integer.bitCount(Byte.toUnsignedInt(btIndex))));
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, short ctIndex, byte btIndex, byte[] popFreqIndex, CtBtCombination ctBtCombination) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.btIndex = btIndex;
        this.ctBtCombination = ctBtCombination == null ? CtBtCombination.empty() : ctBtCombination;
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
        private final byte[] ctBtMatrix;
        private final int numCt;
        private final int numBt;

        public CtBtCombination(byte[] ctBtMatrix, int numCt, int numBt) {
            this.ctBtMatrix = ctBtMatrix;
            this.numCt = numCt;
            this.numBt = numBt;
        }

        public byte[] getCtBtMatrix() {
            return ctBtMatrix;
        }

        public int getNumCt() {
            return numCt;
        }

        public int getNumBt() {
            return numBt;
        }

        public static CtBtCombination empty() {
            return EMPTY;
        }
    }

    public static AnnotationIndexEntry empty(int numPopFreq) {
        return new AnnotationIndexEntry(
                MISSING_ANNOTATION,
                false,
                IndexUtils.EMPTY_MASK,
                IndexUtils.EMPTY_MASK,
                new byte[numPopFreq],
                new byte[0]);
    }

    public byte getSummaryIndex() {
        return summaryIndex;
    }

    public boolean isIntergenic() {
        return intergenic;
    }

    public short getCtIndex() {
        return ctIndex;
    }

    public byte getBtIndex() {
        return btIndex;
    }

    public byte[] getCtBtMatrix() {
        return ctBtCombination.ctBtMatrix;
    }

    public int getNumCts() {
        return ctBtCombination.numCt;
    }

    public int getNumBts() {
        return ctBtCombination.numBt;
    }

    public byte[] getPopFreqIndex() {
        return popFreqIndex;
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
