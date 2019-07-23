package org.opencb.opencga.storage.hadoop.variant.index.annotation;

public class AnnotationIndexEntry {

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
}
