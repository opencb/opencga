package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.CombinationTripleIndexSchema.CombinationTriple;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.List;

public class SampleIndexVariantAnnotation {

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


    public SampleIndexVariantAnnotation() {
    }

    public SampleIndexVariantAnnotation(SampleIndexVariantAnnotation other) {
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

    public SampleIndexVariantAnnotation(
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

    public static SampleIndexVariantAnnotation empty(SampleIndexSchema schema) {
        return new SampleIndexVariantAnnotation()
                .setPopFreqIndex(new BitBuffer(schema.getPopFreqIndex().getBitsLength()))
                .setCtBtTfCombination(new CombinationTriple());
    }

    public boolean hasSummaryIndex() {
        return hasSummaryIndex;
    }

    public SampleIndexVariantAnnotation setHasSummaryIndex(boolean hasSummaryIndex) {
        this.hasSummaryIndex = hasSummaryIndex;
        return this;
    }

    public byte getSummaryIndex() {
        return summaryIndex;
    }

    public SampleIndexVariantAnnotation setSummaryIndex(byte summaryIndex) {
        this.hasSummaryIndex = true;
        this.summaryIndex = summaryIndex;
        return this;
    }

    public boolean isIntergenic() {
        return intergenic;
    }

    public SampleIndexVariantAnnotation setIntergenic(boolean intergenic) {
        this.intergenic = intergenic;
        return this;
    }

    public boolean hasCtIndex() {
        return hasCtIndex;
    }

    public SampleIndexVariantAnnotation setHasCtIndex(boolean hasCtIndex) {
        this.hasCtIndex = hasCtIndex;
        return this;
    }

    public int getCtIndex() {
        return ctIndex;
    }

    public SampleIndexVariantAnnotation setCtIndex(int ctIndex) {
        hasCtIndex = true;
        this.ctIndex = ctIndex;
        return this;
    }

    public boolean hasBtIndex() {
        return hasBtIndex;
    }

    public SampleIndexVariantAnnotation setHasBtIndex(boolean hasBtIndex) {
        this.hasBtIndex = hasBtIndex;
        return this;
    }

    public int getBtIndex() {
        return btIndex;
    }

    public SampleIndexVariantAnnotation setBtIndex(int btIndex) {
        setHasBtIndex(true);
        this.btIndex = btIndex;
        return this;
    }

    public boolean hasTfIndex() {
        return hasTfIndex;
    }

    public SampleIndexVariantAnnotation setHasTfIndex(boolean hasTfIndex) {
        this.hasTfIndex = hasTfIndex;
        return this;
    }

    public int getTfIndex() {
        return tfIndex;
    }

    public SampleIndexVariantAnnotation setTfIndex(int tfIndex) {
        hasTfIndex = true;
        this.tfIndex = tfIndex;
        return this;
    }

    public CombinationTriple getCtBtTfCombination() {
        return ctBtTfCombination;
    }

    public SampleIndexVariantAnnotation setCtBtTfCombination(CombinationTriple ctBtTfCombination) {
        this.ctBtTfCombination = ctBtTfCombination;
        return this;
    }

    public BitBuffer getPopFreqIndex() {
        return popFreqIndex;
    }

    public SampleIndexVariantAnnotation setPopFreqIndex(BitBuffer popFreqIndex) {
        this.popFreqIndex = popFreqIndex;
        return this;
    }

    public boolean hasClinical() {
        return hasClinical;
    }

    public SampleIndexVariantAnnotation setHasClinical(boolean hasClinical) {
        this.hasClinical = hasClinical;
        return this;
    }

    public BitBuffer getClinicalIndex() {
        return clinicalIndex;
    }

    public SampleIndexVariantAnnotation setClinicalIndex(BitBuffer clinicalIndex) {
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
        sb.append(separator).append("clinical: ");
        if (hasClinical()) {
            sb.append(getClinicalIndex());
            List<String> sources = schema.getClinicalIndexSchema().getSourceField().readAndDecode(getClinicalIndex());
            sb.append(" : ").append(sources);
            List<String> clinicalSignificances
                    = schema.getClinicalIndexSchema().getClinicalSignificanceField().readAndDecode(getClinicalIndex());
            sb.append(separator).append("clinicalSignificance: ").append(clinicalSignificances);
        } else {
            sb.append("null");
        }
    }
}
