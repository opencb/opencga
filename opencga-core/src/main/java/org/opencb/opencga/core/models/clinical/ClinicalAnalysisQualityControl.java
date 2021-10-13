package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalComment;

import java.util.List;

public class ClinicalAnalysisQualityControl {

    private QualityControlSummary summary;
    private List<ClinicalComment> comments;

    public ClinicalAnalysisQualityControl() {
    }

    public ClinicalAnalysisQualityControl(QualityControlSummary summary, List<ClinicalComment> comments) {
        this.summary = summary;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQualityControl{");
        sb.append("summary=").append(summary);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public QualityControlSummary getSummary() {
        return summary;
    }

    public ClinicalAnalysisQualityControl setSummary(QualityControlSummary summary) {
        this.summary = summary;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public ClinicalAnalysisQualityControl setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public enum QualityControlSummary {
        HIGH, MEDIUM, LOW, DISCARD, NEEDS_REVIEW, UNKNOWN
    }
}
