package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.alignment.Alignment;
import org.opencb.opencga.core.models.alignment.Coverage;

public class FileQualityControl {
    private Alignment alignment;
    private Coverage coverage;

    public FileQualityControl() {
        this(new Alignment(), new Coverage());
    }

    public FileQualityControl(Alignment alignment, Coverage coverage) {
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

    public Alignment getAlignment() {
        return alignment;
    }

    public FileQualityControl setAlignment(Alignment alignment) {
        this.alignment = alignment;
        return this;
    }

    public Coverage getCoverage() {
        return coverage;
    }

    public FileQualityControl setCoverage(Coverage coverage) {
        this.coverage = coverage;
        return this;
    }
}
