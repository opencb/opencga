package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.opencga.core.models.file.File;

import java.util.List;

public class ClinicalAnalysisVariantQc {
    List<SampleVariantStats> stats;
    List<File> files;

    public ClinicalAnalysisVariantQc() {
    }

    public ClinicalAnalysisVariantQc(List<SampleVariantStats> stats, List<File> files) {
        this.stats = stats;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisVariantQC{");
        sb.append("stats=").append(stats);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public List<SampleVariantStats> getStats() {
        return stats;
    }

    public ClinicalAnalysisVariantQc setStats(List<SampleVariantStats> stats) {
        this.stats = stats;
        return this;
    }

    public List<File> getFiles() {
        return files;
    }

    public ClinicalAnalysisVariantQc setFiles(List<File> files) {
        this.files = files;
        return this;
    }
}
