package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.alignment.AlignmentQualityControl;
import org.opencb.opencga.core.models.alignment.CoverageQualityControl;
import org.opencb.opencga.core.models.variant.VariantFileQualityControl;

public class FileQualityControl {

    private VariantFileQualityControl variant;
    private AlignmentQualityControl alignment;
    private CoverageQualityControl coverage;

    public FileQualityControl() {
        this(new VariantFileQualityControl(), new AlignmentQualityControl(), new CoverageQualityControl());
    }

    @Deprecated
    public FileQualityControl(AlignmentQualityControl alignment, CoverageQualityControl coverage) {
        this.alignment = alignment;
        this.coverage = coverage;
    }

    public FileQualityControl(VariantFileQualityControl variant, AlignmentQualityControl alignment, CoverageQualityControl coverage) {
        this.variant = variant;
        this.alignment = alignment;
        this.coverage = coverage;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileQualityControl{");
        sb.append("variant=").append(variant);
        sb.append(", alignment=").append(alignment);
        sb.append(", coverage=").append(coverage);
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

    public AlignmentQualityControl getAlignment() {
        return alignment;
    }

    public FileQualityControl setAlignment(AlignmentQualityControl alignment) {
        this.alignment = alignment;
        return this;
    }

    public CoverageQualityControl getCoverage() {
        return coverage;
    }

    public FileQualityControl setCoverage(CoverageQualityControl coverage) {
        this.coverage = coverage;
        return this;
    }
}
