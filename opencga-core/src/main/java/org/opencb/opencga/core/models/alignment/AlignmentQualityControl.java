package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AlignmentQualityControl implements Serializable {
    private String fileId;
    private FastQc fastQc;
    private SamtoolsStats samtoolsStats;
    private SamtoolsFlagstats samtoolsFlagstats;
    private HsMetrics hsMetrics;
    private List<GeneCoverageStats> geneCoverageStats;

    public AlignmentQualityControl() {
        this(null, new FastQc(), new SamtoolsStats(), new SamtoolsFlagstats(), new HsMetrics(), new ArrayList<>());
    }

    public AlignmentQualityControl(String fileId, FastQc fastQc, SamtoolsStats samtoolsStats, SamtoolsFlagstats samtoolsFlagstats,
                                   HsMetrics hsMetrics, List<GeneCoverageStats> geneCoverageStats) {
        this.fileId = fileId;
        this.fastQc = fastQc;
        this.samtoolsStats = samtoolsStats;
        this.samtoolsFlagstats = samtoolsFlagstats;
        this.hsMetrics = hsMetrics;
        this.geneCoverageStats = geneCoverageStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentQualityControl{");
        sb.append("fileId='").append(fileId).append('\'');
        sb.append(", fastQc=").append(fastQc);
        sb.append(", samtoolsStats=").append(samtoolsStats);
        sb.append(", samtoolsFlagstats=").append(samtoolsFlagstats);
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

    public FastQc getFastQc() {
        return fastQc;
    }

    public AlignmentQualityControl setFastQc(FastQc fastQc) {
        this.fastQc = fastQc;
        return this;
    }

    public SamtoolsStats getSamtoolsStats() {
        return samtoolsStats;
    }

    public AlignmentQualityControl setSamtoolsStats(SamtoolsStats samtoolsStats) {
        this.samtoolsStats = samtoolsStats;
        return this;
    }

    public SamtoolsFlagstats getSamtoolsFlagstats() {
        return samtoolsFlagstats;
    }

    public AlignmentQualityControl setSamtoolsFlagstats(SamtoolsFlagstats samtoolsFlagstats) {
        this.samtoolsFlagstats = samtoolsFlagstats;
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
