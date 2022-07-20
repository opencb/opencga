package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.opencga.core.models.file.File;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisVariantQc {

    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_VARIANT_QC_STATS_DESCRIPTION)
    private List<SampleQcVariantStats> stats;
    @DataField(description = ParamConstants.CLINICAL_ANALYSIS_VARIANT_QC_FILES_DESCRIPTION)
    private List<File> files;

    public ClinicalAnalysisVariantQc() {
    }

    public ClinicalAnalysisVariantQc(List<SampleQcVariantStats> stats, List<File> files) {
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

    public List<SampleQcVariantStats> getStats() {
        return stats;
    }

    public ClinicalAnalysisVariantQc setStats(List<SampleQcVariantStats> stats) {
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
