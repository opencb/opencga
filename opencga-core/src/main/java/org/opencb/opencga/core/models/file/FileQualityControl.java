package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.alignment.AlignmentQualityControl;
import org.opencb.opencga.core.models.alignment.CoverageQualityControl;

public class FileQualityControl {
    private AlignmentQualityControl alignmentQualityControl;
    private CoverageQualityControl coverageQualityControl;

    public FileQualityControl() {
        this(new AlignmentQualityControl(), new CoverageQualityControl());
    }

    public FileQualityControl(AlignmentQualityControl alignmentQualityControl, CoverageQualityControl coverageQualityControl) {
        this.alignmentQualityControl = alignmentQualityControl;
        this.coverageQualityControl = coverageQualityControl;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileQualityControl{");
        sb.append("alignmentQualityControl=").append(alignmentQualityControl);
        sb.append(", coverageQualityControl=").append(coverageQualityControl);
        sb.append('}');
        return sb.toString();
    }

    public AlignmentQualityControl getAlignmentQualityControl() {
        return alignmentQualityControl;
    }

    public FileQualityControl setAlignmentQualityControl(AlignmentQualityControl alignmentQualityControl) {
        this.alignmentQualityControl = alignmentQualityControl;
        return this;
    }

    public CoverageQualityControl getCoverageQualityControl() {
        return coverageQualityControl;
    }

    public FileQualityControl setCoverageQualityControl(CoverageQualityControl coverageQualityControl) {
        this.coverageQualityControl = coverageQualityControl;
        return this;
    }
}
