package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

public class AnnotationIndexEntry {

    public static final byte MISSING_ANNOTATION = (byte) 0xFF;
    private final byte summaryIndex;
    private final boolean intergenic;
    private final short ctIndex;
    private final byte btIndex;
    private final byte[] popFreqIndex;


    public AnnotationIndexEntry(byte summaryIndex, boolean intergenic, short ctIndex, byte btIndex, byte[] popFreqIndex) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.btIndex = btIndex;
        this.popFreqIndex = popFreqIndex;
    }

    public static AnnotationIndexEntry empty(int numPopFreq) {
        return new AnnotationIndexEntry(MISSING_ANNOTATION, false, IndexUtils.EMPTY_MASK, IndexUtils.EMPTY_MASK, new byte[numPopFreq]);
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
