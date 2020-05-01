package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.alignment.AlignmentStats;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.opencga.core.models.file.File;

import java.util.List;

public class ClinicalAnalysisAlignmentQC {

    List<AlignmentStats> stats;
    List<GeneCoverageStats> geneCoverageStats;
    List<File> files;

    public ClinicalAnalysisAlignmentQC() {
    }

    public ClinicalAnalysisAlignmentQC(List<AlignmentStats> stats, List<GeneCoverageStats> geneCoverageStats, List<File> files) {
        this.stats = stats;
        this.geneCoverageStats = geneCoverageStats;
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisAlignmentQC{");
        sb.append("stats=").append(stats);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public List<AlignmentStats> getStats() {
        return stats;
    }

    public ClinicalAnalysisAlignmentQC setStats(List<AlignmentStats> stats) {
        this.stats = stats;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public ClinicalAnalysisAlignmentQC setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }

    public List<File> getFiles() {
        return files;
    }

    public ClinicalAnalysisAlignmentQC setFiles(List<File> files) {
        this.files = files;
        return this;
    }
}
