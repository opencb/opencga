package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.CombinationTripleIndexSchema.CombinationTriple;
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
    private CombinationTriple ctBtTfCombination;
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
        this.ctBtTfCombination = new CombinationTriple(other.ctBtTfCombination);
        this.popFreqIndex = other.popFreqIndex == null ? null : new BitBuffer(other.popFreqIndex);
        this.hasClinical = other.hasClinical;
        this.clinicalIndex = other.clinicalIndex == null ? null : new BitBuffer(other.clinicalIndex);
    }

    public AnnotationIndexEntry(
            byte summaryIndex, boolean intergenic, int ctIndex, int btIndex, int tfIndex,
            CombinationTriple ctBtTfCombination, BitBuffer popFreqIndex,
            boolean hasClinical, BitBuffer clinicalIndex) {
        this.summaryIndex = summaryIndex;
        this.intergenic = intergenic;
        this.ctIndex = ctIndex;
        this.hasCtIndex = !intergenic;
        this.btIndex = btIndex;
        this.hasBtIndex = !intergenic;
        this.tfIndex = tfIndex;
        this.hasTfIndex = !intergenic;
        this.ctBtTfCombination = ctBtTfCombination == null ? new CombinationTriple() : ctBtTfCombination;
        this.hasClinical = hasClinical;
        this.clinicalIndex = clinicalIndex;
        this.popFreqIndex = popFreqIndex;
    }

    public static AnnotationIndexEntry empty(SampleIndexSchema schema) {
        return new AnnotationIndexEntry()
                .setPopFreqIndex(new BitBuffer(schema.getPopFreqIndex().getBitsLength()))
                .setCtBtTfCombination(new CombinationTriple());
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

    public CombinationTriple getCtBtTfCombination() {
        return ctBtTfCombination;
    }

    public AnnotationIndexEntry setCtBtTfCombination(CombinationTriple ctBtTfCombination) {
        this.ctBtTfCombination = ctBtTfCombination;
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
        summaryIndex = 0;
        ctIndex = 0;
        btIndex = 0;
        tfIndex = 0;
        ctBtTfCombination.setNumX(0);
        ctBtTfCombination.setNumY(0);
        ctBtTfCombination.setNumZ(0);
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


    public void toString(SampleIndexSchema schema, String separator, StringBuilder sb) {
        sb.append(separator).append("ct: ")
                .append(IndexUtils.binaryToString(
                        getCtIndex(),
                        schema.getCtIndex().getField().getBitLength()))
                .append(" : ")
                .append(schema.getCtIndex().getField()
                        .decode(getCtIndex()));
        sb.append(separator).append("bt: ")
                .append(IndexUtils.binaryToString(
                        getBtIndex(),
                        schema.getBiotypeIndex().getField().getBitLength()))
                .append(" : ")
                .append(schema.getBiotypeIndex().getField()
                        .decode(getBtIndex()));
        sb.append(separator).append("tf: ")
                .append(IndexUtils.binaryToString(
                        getTfIndex(),
                        schema.getTranscriptFlagIndexSchema().getField().getBitLength()))
                .append(" : ")
                .append(schema.getTranscriptFlagIndexSchema().getField()
                        .decode(getTfIndex()));
        sb.append(separator).append("ct_bt_tf: ")
                .append(schema.getCtBtTfIndex().getField()
                        .encode(getCtBtTfCombination()))
                .append(" : ")
                .append(getCtBtTfCombination())
                .append(" : ")
                .append(schema.getCtBtTfIndex().getField()
                        .getTriples(
                                getCtBtTfCombination(),
                                getCtIndex(),
                                getBtIndex(),
                                getTfIndex()));
    }
}
