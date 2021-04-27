package org.opencb.opencga.core.models.sample;

import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.util.ArrayList;
import java.util.List;

public class SampleAlignmentQualityControlMetrics {

    private String bamFileId;
    private FastQcMetrics fastQcMetrics;
    private SamtoolsFlagstats samtoolsFlagstats;
    private HsMetrics hsMetrics;
    private List<GeneCoverageStats> geneCoverageStats;

    public SampleAlignmentQualityControlMetrics() {
        this(null, null, null, null, new ArrayList<>());
    }

    public SampleAlignmentQualityControlMetrics(String bamFileId, FastQcMetrics fastQcMetrics, SamtoolsFlagstats samtoolsFlagstats, HsMetrics hsMetrics,
                                                List<GeneCoverageStats> geneCoverageStats) {
        this.bamFileId = bamFileId;
        this.fastQcMetrics = fastQcMetrics;
        this.samtoolsFlagstats = samtoolsFlagstats;
        this.hsMetrics = hsMetrics;
        this.geneCoverageStats = geneCoverageStats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleAlignmentQualityControlMetrics{");
        sb.append("bamFileId='").append(bamFileId).append('\'');
        sb.append(", fastQcMetrics=").append(fastQcMetrics);
        sb.append(", samtoolsFlagstats=").append(samtoolsFlagstats);
        sb.append(", hsMetrics=").append(hsMetrics);
        sb.append(", geneCoverageStats=").append(geneCoverageStats);
        sb.append('}');
        return sb.toString();
    }

    public String getBamFileId() {
        return bamFileId;
    }

    public SampleAlignmentQualityControlMetrics setBamFileId(String bamFileId) {
        this.bamFileId = bamFileId;
        return this;
    }

    public FastQcMetrics getFastQc() {
        return fastQcMetrics;
    }

    public SampleAlignmentQualityControlMetrics setFastQc(FastQcMetrics fastQcMetrics) {
        this.fastQcMetrics = fastQcMetrics;
        return this;
    }

    public SamtoolsFlagstats getSamtoolsFlagstats() {
        return samtoolsFlagstats;
    }

    public SampleAlignmentQualityControlMetrics setSamtoolsFlagstats(SamtoolsFlagstats samtoolsFlagstats) {
        this.samtoolsFlagstats = samtoolsFlagstats;
        return this;
    }

    public HsMetrics getHsMetrics() {
        return hsMetrics;
    }

    public SampleAlignmentQualityControlMetrics setHsMetrics(HsMetrics hsMetrics) {
        this.hsMetrics = hsMetrics;
        return this;
    }

    public List<GeneCoverageStats> getGeneCoverageStats() {
        return geneCoverageStats;
    }

    public SampleAlignmentQualityControlMetrics setGeneCoverageStats(List<GeneCoverageStats> geneCoverageStats) {
        this.geneCoverageStats = geneCoverageStats;
        return this;
    }
}
