package org.opencb.opencga.core.models.clinical;

public class ClinicalAnalysisQC {

    private String qcResult;
    private ClinicalAnalysisVariantQC variant;
    private ClinicalAnalysisAlignmentQC alignment;

    public ClinicalAnalysisQC() {
    }

    public ClinicalAnalysisQC(String qcResult, ClinicalAnalysisVariantQC variant, ClinicalAnalysisAlignmentQC alignment) {
        this.qcResult = qcResult;
        this.variant = variant;
        this.alignment = alignment;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQC{");
        sb.append("qcResult='").append(qcResult).append('\'');
        sb.append(", variant=").append(variant);
        sb.append(", alignment=").append(alignment);
        sb.append('}');
        return sb.toString();
    }

    public String getQcResult() {
        return qcResult;
    }

    public ClinicalAnalysisQC setQcResult(String qcResult) {
        this.qcResult = qcResult;
        return this;
    }

    public ClinicalAnalysisVariantQC getVariant() {
        return variant;
    }

    public ClinicalAnalysisQC setVariant(ClinicalAnalysisVariantQC variant) {
        this.variant = variant;
        return this;
    }

    public ClinicalAnalysisAlignmentQC getAlignment() {
        return alignment;
    }

    public ClinicalAnalysisQC setAlignment(ClinicalAnalysisAlignmentQC alignment) {
        this.alignment = alignment;
        return this;
    }
}
