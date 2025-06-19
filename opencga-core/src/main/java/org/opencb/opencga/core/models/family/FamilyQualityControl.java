package org.opencb.opencga.core.models.family;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.qc.Relatedness;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FamilyQualityControl implements Serializable {

    @DataField(id = "relatedness", description = FieldConstants.FAMILY_QUALITY_CONTROL_RELATEDNESS_REPORT_DESCRIPTION)
    private List<Relatedness> relatedness;

    @DataField(id = "files", description = FieldConstants.QC_FILES_DESCRIPTION)
    private List<String> files;

    @DataField(id = "comments", description = FieldConstants.QC_COMMENTS_DESCRIPTION)
    private List<ClinicalComment> comments;

    @DataField(id = "attributes", description = FieldConstants.QC_ATTRIBUTES_DESCRIPTION)
    private ObjectMap attributes;

    public FamilyQualityControl() {
        this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ObjectMap());
    }

    @Deprecated
    public FamilyQualityControl(List<Relatedness> relatedness, List<String> files, List<ClinicalComment> comments) {
        this.relatedness = relatedness;
        this.files = files;
        this.comments = comments;
    }

    public FamilyQualityControl(List<Relatedness> relatedness, List<String> files, List<ClinicalComment> comments, ObjectMap attributes) {
        this.relatedness = relatedness;
        this.files = files;
        this.comments = comments;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyQualityControl{");
        sb.append("relatedness=").append(relatedness);
        sb.append(", files=").append(files);
        sb.append(", comments=").append(comments);
        sb.append(", attributes=").append(attributes);
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

    public ObjectMap getAttributes() {
        return attributes;
    }

    public FamilyQualityControl setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
