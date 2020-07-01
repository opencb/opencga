package org.opencb.opencga.core.models.family;

import org.opencb.biodata.models.clinical.Comment;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FamilyQualityControl implements Serializable {
    private List<RelatednessReport> relatedness;
    private List<String> fileIds;
    private List<Comment> comments;

    public FamilyQualityControl() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public FamilyQualityControl(List<RelatednessReport> relatedness, List<String> fileIds, List<Comment> comments) {
        this.relatedness = relatedness;
        this.fileIds = fileIds;
        this.comments = comments;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQualityControl{");
        sb.append("relatedness=").append(relatedness);
        sb.append(", fileIds=").append(fileIds);
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

    public List<String> getFileIds() {
        return fileIds;
    }

    public FamilyQualityControl setFileIds(List<String> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public List<Comment> getComments() {
        return comments;
    }

    public FamilyQualityControl setComments(List<Comment> comments) {
        this.comments = comments;
        return this;
    }
}
