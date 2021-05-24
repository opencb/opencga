package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.alignment.AlignmentQualityControl;
import org.opencb.opencga.core.models.alignment.CoverageQualityControl;

public class FileQualityControl {
    private AlignmentQualityControl alignment;
    private CoverageQualityControl coverage;

    public FileQualityControl() {
        this(new AlignmentQualityControl(), new CoverageQualityControl());
    }

    public FileQualityControl(AlignmentQualityControl alignment, CoverageQualityControl coverage) {
        this.alignment = alignment;
        this.coverage = coverage;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileQualityControl{");
        sb.append("alignment=").append(alignment);
        sb.append(", coverage=").append(coverage);
        sb.append('}');
        return sb.toString();
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
