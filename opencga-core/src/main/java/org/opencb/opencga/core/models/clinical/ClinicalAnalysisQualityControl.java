package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;

public class ClinicalAnalysisQualityControl {

    @DataField(id = "summary", description = FieldConstants.CLINICAL_ANALYSIS_QUALITY_CONTROL_SUMMARY)
    private QualityControlSummary summary;

    @DataField(id = "comments", description = FieldConstants.CLINICAL_ANALYSIS_QUALITY_CONTROL_COMMENTS)
    private List<ClinicalComment> comments;

    @DataField(id = "files", description = FieldConstants.CLINICAL_ANALYSIS_QUALITY_CONTROL_FILES)
    private List<String> files;

    public enum QualityControlSummary {
        HIGH, MEDIUM, LOW, CRITICAL, UNKNOWN
    }

    public ClinicalAnalysisQualityControl() {
    }

    public ClinicalAnalysisQualityControl(QualityControlSummary summary, List<ClinicalComment> comments, List<String> files) {
        this.summary = summary;
        this.comments = comments;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQualityControl{");
        sb.append("summary=").append(summary);
        sb.append(", comments=").append(comments);
        sb.append(", files=").append(files);
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

    public List<String> getFiles() {
        return files;
    }

    public ClinicalAnalysisQualityControl setFiles(List<String> files) {
        this.files = files;
        return this;
    }

}
