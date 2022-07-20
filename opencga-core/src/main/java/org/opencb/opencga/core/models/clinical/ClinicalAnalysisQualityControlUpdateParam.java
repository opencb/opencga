package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisQualityControlUpdateParam {

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_QUALITY_CONTROL_UPDATE_PARAM_SUMMARY_DESCRIPTION)
    private ClinicalAnalysisQualityControl.QualityControlSummary summary;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_QUALITY_CONTROL_UPDATE_PARAM_COMMENTS_DESCRIPTION)
    private List<String> comments;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_QUALITY_CONTROL_UPDATE_PARAM_FILES_DESCRIPTION)
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
