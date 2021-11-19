package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClinicalAnalysisQualityControlUpdateParam {

    private ClinicalAnalysisQualityControl.QualityControlSummary summary;
    private List<String> comments;
    private List<String> files;

    public ClinicalAnalysisQualityControlUpdateParam() {
    }

    public ClinicalAnalysisQualityControlUpdateParam(ClinicalAnalysisQualityControl.QualityControlSummary summary, List<String> comments,
                                                     List<String> files) {
        this.summary = summary;
        this.comments = comments;
        this.files = files;
    }

    public static ClinicalAnalysisQualityControlUpdateParam of(ClinicalAnalysisQualityControl qualityControl) {
        List<String> tmpComments = new ArrayList<>();
        if (qualityControl.getComments() != null) {
            for (ClinicalComment comment : qualityControl.getComments()) {
                tmpComments.add(comment.getMessage());
            }
        }

        return new ClinicalAnalysisQualityControlUpdateParam(qualityControl.getSummary(), tmpComments, qualityControl.getFiles());
    }

    public ClinicalAnalysisQualityControl toClinicalQualityControl() {
        List<ClinicalComment> tmpComments = new ArrayList<>();
        if (comments != null) {
            for (String comment : comments) {
                tmpComments.add(new ClinicalComment("", comment, Collections.emptyList(), TimeUtils.getTime()));
            }
        }
        return new ClinicalAnalysisQualityControl(summary, tmpComments, files);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisQualityControlUpdateParam{");
        sb.append("summary=").append(summary);
        sb.append(", comments=").append(comments);
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
}
