package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;

import java.io.Serializable;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class AlignmentFileQualityControl implements Serializable {

    @DataField(description = ParamConstants.ALIGNMENT_FILE_QUALITY_CONTROL_FAST_QC_METRICS_DESCRIPTION)
    private FastQcMetrics fastQcMetrics;
    @DataField(description = ParamConstants.ALIGNMENT_FILE_QUALITY_CONTROL_SAMTOOLS_STATS_DESCRIPTION)
    private SamtoolsStats samtoolsStats;
    @DataField(description = ParamConstants.ALIGNMENT_FILE_QUALITY_CONTROL_SAMTOOLS_FLAG_STATS_DESCRIPTION)
    private SamtoolsFlagstats samtoolsFlagStats;
    @DataField(description = ParamConstants.ALIGNMENT_FILE_QUALITY_CONTROL_HS_METRICS_DESCRIPTION)
    private HsMetrics hsMetrics;

    public AlignmentFileQualityControl() {
        this(null, null, null, null);
    }

    public AlignmentFileQualityControl(FastQcMetrics fastQcMetrics, SamtoolsStats samtoolsStats, SamtoolsFlagstats samtoolsFlagStats,
                                       HsMetrics hsMetrics) {
        this.fastQcMetrics = fastQcMetrics;
        this.samtoolsStats = samtoolsStats;
        this.samtoolsFlagStats = samtoolsFlagStats;
        this.hsMetrics = hsMetrics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Alignment{");
        sb.append("fastQcMetrics=").append(fastQcMetrics);
        sb.append(", samtoolsStats=").append(samtoolsStats);
        sb.append(", samtoolsFlagStats=").append(samtoolsFlagStats);
        sb.append(", hsMetrics=").append(hsMetrics);
        sb.append('}');
        return sb.toString();
    }

    public FastQcMetrics getFastQcMetrics() {
        return fastQcMetrics;
    }

    public AlignmentFileQualityControl setFastQcMetrics(FastQcMetrics fastQcMetrics) {
        this.fastQcMetrics = fastQcMetrics;
        return this;
    }

    public SamtoolsStats getSamtoolsStats() {
        return samtoolsStats;
    }

    public AlignmentFileQualityControl setSamtoolsStats(SamtoolsStats samtoolsStats) {
        this.samtoolsStats = samtoolsStats;
        return this;
    }

    public SamtoolsFlagstats getSamtoolsFlagStats() {
        return samtoolsFlagStats;
    }

    public AlignmentFileQualityControl setSamtoolsFlagStats(SamtoolsFlagstats samtoolsFlagStats) {
        this.samtoolsFlagStats = samtoolsFlagStats;
        return this;
    }

    public HsMetrics getHsMetrics() {
        return hsMetrics;
    }

    public AlignmentFileQualityControl setHsMetrics(HsMetrics hsMetrics) {
        this.hsMetrics = hsMetrics;
        return this;
    }
}
