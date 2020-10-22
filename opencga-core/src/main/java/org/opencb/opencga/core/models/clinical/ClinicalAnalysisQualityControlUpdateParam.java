package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.common.TimeUtils;

public class ClinicalAnalysisQualityControlUpdateParam {

    private ClinicalAnalysisQualityControl.QualityControlSummary summary;
    private String comment;

    public ClinicalAnalysisQualityControlUpdateParam() {
    }

    public ClinicalAnalysisQualityControlUpdateParam(ClinicalAnalysisQualityControl.QualityControlSummary summary, String comment) {
        this.summary = summary;
        this.comment = comment;
    }

    public ClinicalAnalysisQualityControl toClinicalQualityControl() {
        return new ClinicalAnalysisQualityControl(summary, comment, "", TimeUtils.getDate());
    }

    public static ClinicalAnalysisQualityControlUpdateParam of(ClinicalAnalysisQualityControl qualityControl) {
        return new ClinicalAnalysisQualityControlUpdateParam(qualityControl.getSummary(), qualityControl.getComment());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQualityControlUpdateParam{");
        sb.append("summary=").append(summary);
        sb.append(", comment='").append(comment).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public ClinicalAnalysisQualityControl.QualityControlSummary getSummary() {
        return summary;
    }

    public ClinicalAnalysisQualityControlUpdateParam setSummary(ClinicalAnalysisQualityControl.QualityControlSummary summary) {
        this.summary = summary;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public ClinicalAnalysisQualityControlUpdateParam setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
