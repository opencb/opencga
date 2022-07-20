package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.models.alignment.GeneCoverageStats;
import org.opencb.opencga.core.models.file.File;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisAlignmentQc {

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_ALIGNMENT_QC_STATS_DESCRIPTION)
    private List<SamtoolsStats> stats;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_ALIGNMENT_QC_GENE_COVERAGE_STATS_DESCRIPTION)
    private List<GeneCoverageStats> geneCoverageStats;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_ALIGNMENT_QC_FILES_DESCRIPTION)
    private List<File> files;

    public ClinicalAnalysisAlignmentQc() {
    }

    public ClinicalAnalysisAlignmentQc(List<SamtoolsStats> stats, List<GeneCoverageStats> geneCoverageStats, List<File> files) {
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

    public List<SamtoolsStats> getStats() {
        return stats;
    }

    public ClinicalAnalysisAlignmentQc setStats(List<SamtoolsStats> stats) {
        this.stats = stats;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public ClinicalAnalysisAlignmentQc setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }

    public List<File> getFiles() {
        return files;
    }

    public ClinicalAnalysisAlignmentQc setFiles(List<File> files) {
        this.files = files;
        return this;
    }
}
