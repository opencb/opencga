package org.opencb.opencga.core.models.family;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.qc.Relatedness;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FamilyQualityControl implements Serializable {

    @DataField(id = "relatedness",
            description = FieldConstants.FAMILY_QUALITY_CONTROL_RELATEDNESS_REPORT_DESCRIPTION)
    private List<Relatedness> relatedness;

    @DataField(id = "files",
            description = FieldConstants.QC_FILES_DESCRIPTION)
    private List<String> files;

    @DataField(id = "comments",
            description = FieldConstants.QC_COMMENTS_DESCRIPTION)
    private List<ClinicalComment> comments;

    public FamilyQualityControl() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public FamilyQualityControl(List<Relatedness> relatedness, List<String> files, List<ClinicalComment> comments) {
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

    public List<Relatedness> getRelatedness() {
        return relatedness;
    }

    public FamilyQualityControl setRelatedness(List<Relatedness> relatedness) {
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
