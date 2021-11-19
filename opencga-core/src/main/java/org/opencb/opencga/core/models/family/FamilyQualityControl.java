package org.opencb.opencga.core.models.family;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FamilyQualityControl implements Serializable {
    private List<RelatednessReport> relatedness;
    private List<String> files;
    private List<ClinicalComment> comments;

    public FamilyQualityControl() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public FamilyQualityControl(List<RelatednessReport> relatedness, List<String> files, List<ClinicalComment> comments) {
        this.relatedness = relatedness;
        this.files = files;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQualityControl{");
        sb.append("relatedness=").append(relatedness);
        sb.append(", files=").append(files);
        sb.append(", comments=").append(comments);
        sb.append('}');
        return sb.toString();
    }

    public List<RelatednessReport> getRelatedness() {
        return relatedness;
    }

    public FamilyQualityControl setRelatedness(List<RelatednessReport> relatedness) {
        this.relatedness = relatedness;
        return this;
    }

    public List<String> getFiles() {
        return files;
    }

    public FamilyQualityControl setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public FamilyQualityControl setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }
}
