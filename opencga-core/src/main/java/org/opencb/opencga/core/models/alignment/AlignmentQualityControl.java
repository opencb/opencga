package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AlignmentQualityControl implements Serializable {
    private String fileId;
    private FastQcMetrics fastQcMetrics;
    private SamtoolsStats samtoolsStats;
    private SamtoolsFlagstats samtoolsFlagStats;
    private HsMetrics hsMetrics;
    private List<GeneCoverageStats> geneCoverageStats;

    public AlignmentQualityControl() {
        this(null, new FastQcMetrics(), new SamtoolsStats(), new SamtoolsFlagstats(), new HsMetrics(), new ArrayList<>());
    }

    public AlignmentQualityControl(String fileId, FastQcMetrics fastQcMetrics, SamtoolsStats samtoolsStats, SamtoolsFlagstats samtoolsFlagStats,
                                   HsMetrics hsMetrics, List<GeneCoverageStats> geneCoverageStats) {
        this.fileId = fileId;
        this.fastQcMetrics = fastQcMetrics;
        this.samtoolsStats = samtoolsStats;
        this.samtoolsFlagStats = samtoolsFlagStats;
        this.hsMetrics = hsMetrics;
        this.geneCoverageStats = geneCoverageStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentQualityControl{");
        sb.append("fileId='").append(fileId).append('\'');
        sb.append(", fastQcMetrics=").append(fastQcMetrics);
        sb.append(", samtoolsStats=").append(samtoolsStats);
        sb.append(", samtoolsFlagstats=").append(samtoolsFlagStats);
        sb.append(", hsMetrics=").append(hsMetrics);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append('}');
        return sb.toString();
    }

    public String getFileId() {
        return fileId;
    }

    public AlignmentQualityControl setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public FastQcMetrics getFastQcMetrics() {
        return fastQcMetrics;
    }

    public AlignmentQualityControl setFastQcMetrics(FastQcMetrics fastQcMetrics) {
        this.fastQcMetrics = fastQcMetrics;
        return this;
    }

    public SamtoolsStats getSamtoolsStats() {
        return samtoolsStats;
    }

    public AlignmentQualityControl setSamtoolsStats(SamtoolsStats samtoolsStats) {
        this.samtoolsStats = samtoolsStats;
        return this;
    }

    public SamtoolsFlagstats getSamtoolsFlagStats() {
        return samtoolsFlagStats;
    }

    public AlignmentQualityControl setSamtoolsFlagStats(SamtoolsFlagstats samtoolsFlagStats) {
        this.samtoolsFlagStats = samtoolsFlagStats;
        return this;
    }

    public HsMetrics getHsMetrics() {
        return hsMetrics;
    }

    public AlignmentQualityControl setHsMetrics(HsMetrics hsMetrics) {
        this.hsMetrics = hsMetrics;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public AlignmentQualityControl setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }
}
