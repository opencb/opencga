package org.opencb.opencga.core.models.file;

import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.opencga.core.models.alignment.AlignmentFileQualityControl;
import org.opencb.opencga.core.models.alignment.CoverageFileQualityControl;
import org.opencb.opencga.core.models.variant.VariantFileQualityControl;

import java.util.ArrayList;
import java.util.List;

public class FileQualityControl {

    private VariantFileQualityControl variant;
    private AlignmentFileQualityControl alignment;
    private CoverageFileQualityControl coverage;
    private List<ClinicalComment> comments;
    private List<String> files;

    public FileQualityControl() {
        this(new VariantFileQualityControl(), new AlignmentFileQualityControl(), new CoverageFileQualityControl(), new ArrayList<>(),
                new ArrayList<>());
    }

    @Deprecated
    public FileQualityControl(AlignmentFileQualityControl alignment, CoverageFileQualityControl coverage) {
        this.alignment = alignment;
        this.coverage = coverage;
    }

    public FileQualityControl(VariantFileQualityControl variant, AlignmentFileQualityControl alignment, CoverageFileQualityControl coverage,
                              List<ClinicalComment> comments, List<String> files) {
        this.variant = variant;
        this.alignment = alignment;
        this.coverage = coverage;
        this.comments = comments;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileQualityControl{");
        sb.append("variant=").append(variant);
        sb.append(", alignment=").append(alignment);
        sb.append(", coverage=").append(coverage);
        sb.append(", comments=").append(comments);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public VariantFileQualityControl getVariant() {
        return variant;
    }

    public FileQualityControl setVariant(VariantFileQualityControl variant) {
        this.variant = variant;
        return this;
    }

    public AlignmentFileQualityControl getAlignment() {
        return alignment;
    }

    public FileQualityControl setAlignment(AlignmentFileQualityControl alignment) {
        this.alignment = alignment;
        return this;
    }

    public CoverageFileQualityControl getCoverage() {
        return coverage;
    }

    public FileQualityControl setCoverage(CoverageFileQualityControl coverage) {
        this.coverage = coverage;
        return this;
    }

    public List<ClinicalComment> getComments() {
        return comments;
    }

    public FileQualityControl setComments(List<ClinicalComment> comments) {
        this.comments = comments;
        return this;
    }

    public List<String> getFiles() {
        return files;
    }

    public FileQualityControl setFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
