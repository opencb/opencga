package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
    private CtBtCombination ctBtCombination;
    private BitBuffer popFreqIndex;
    private boolean hasClinical;
    private BitBuffer clinicalIndex;


    public AnnotationIndexEntry() {
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, int ctIndex, int btIndex, CtBtCombination ctBtCombination, BitBuffer popFreqIndex,
            boolean hasClinical, BitBuffer clinicalIndex) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.btIndex = btIndex;
        this.ctBtCombination = ctBtCombination == null ? CtBtCombination.empty() : ctBtCombination;
        this.hasClinical = hasClinical;
        this.clinicalIndex = clinicalIndex;
        this.popFreqIndex = popFreqIndex;
    }


    /**
     * Matrix that contains the ConsequenceType and Biotype transcript combinations in one variant.
     *
     * Each non-intergenic variant has a set of pairs (CT, BT), which defines a list of CT and BT values.
     * This can be represented as a matrix where the rows are CT values in the variant, the columns are BT values,
     * and the intersection is a boolean representing if that specific combination occurs in the variant.
     * <pre>
     *       +---------+---------+----+
     *       | bt1     | bt2     | ...|
     * +-----+---------+---------+----+
     * | ct1 | ct1-bt1 | ct1-bt2 |    |
     * | ct2 | ct2+bt1 | ct2+bt2 |    |
     * | ... |         |         | ...|
     * +-----+---------+---------+----+
     * </pre>
     *
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
        public static final CtBtCombination EMPTY = new CtBtCombination(new int[0], 0, 0);
        private int[] ctBtMatrix;
        private int numCt;
        private int numBt;

        public CtBtCombination(int[] ctBtMatrix, int numCt, int numBt) {
            this.ctBtMatrix = ctBtMatrix;
            this.numCt = numCt;
            this.numBt = numBt;
        }

        public int[] getCtBtMatrix() {
            return ctBtMatrix;
        }

        public CtBtCombination setCtBtMatrix(int[] ctBtMatrix) {
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

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CtBtCombination{");
            sb.append("numCt=").append(numCt);
            sb.append(", numBt=").append(numBt);
            sb.append(", ctBtMatrix=").append(IntStream.of(ctBtMatrix)
                    .mapToObj(i -> IndexUtils.binaryToString(i, numBt))
                    .collect(Collectors.joining(", ", "[", "]")));
            sb.append('}');
            return sb.toString();
        }
    }

    public static AnnotationIndexEntry empty() {
        return new AnnotationIndexEntry().setCtBtCombination(new CtBtCombination(new int[0], 0, 0));
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

    public CtBtCombination getCtBtCombination() {
        return ctBtCombination;
    }

    public AnnotationIndexEntry setCtBtCombination(CtBtCombination ctBtCombination) {
        this.ctBtCombination = ctBtCombination;
        return this;
    }

    public BitBuffer getPopFreqIndex() {
        return popFreqIndex;
    }

    public AnnotationIndexEntry setPopFreqIndex(BitBuffer popFreqIndexBB) {
        this.popFreqIndex = popFreqIndexBB;
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
