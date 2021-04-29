package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.alignment.AlignmentQualityControl;

public class FileQualityControl {
    private AlignmentQualityControl alignmentQualityControl;

    public FileQualityControl() {
        this(new AlignmentQualityControl());
    }

    public FileQualityControl(AlignmentQualityControl alignmentQualityControl) {
        this.alignmentQualityControl = alignmentQualityControl;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileQualityControl{");
        sb.append("alignmentQualityControl=").append(alignmentQualityControl);
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
}
