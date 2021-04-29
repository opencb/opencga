package org.opencb.opencga.core.models.alignment;

import org.opencb.biodata.formats.alignment.picard.HsMetrics;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.models.alignment.GeneCoverageStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Alignment implements Serializable {
    private FastQcMetrics fastQcMetrics;
    private SamtoolsStats samtoolsStats;
    private SamtoolsFlagstats samtoolsFlagStats;
    private HsMetrics hsMetrics;

    public Alignment() {
        this(new FastQcMetrics(), new SamtoolsStats(), new SamtoolsFlagstats(), new HsMetrics());
    }

    public Alignment(FastQcMetrics fastQcMetrics, SamtoolsStats samtoolsStats, SamtoolsFlagstats samtoolsFlagStats, HsMetrics hsMetrics) {
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

    public Alignment setFastQcMetrics(FastQcMetrics fastQcMetrics) {
        this.fastQcMetrics = fastQcMetrics;
        return this;
    }

    public SamtoolsStats getSamtoolsStats() {
        return samtoolsStats;
    }

    public Alignment setSamtoolsStats(SamtoolsStats samtoolsStats) {
        this.samtoolsStats = samtoolsStats;
        return this;
    }

    public SamtoolsFlagstats getSamtoolsFlagStats() {
        return samtoolsFlagStats;
    }

    public Alignment setSamtoolsFlagStats(SamtoolsFlagstats samtoolsFlagStats) {
        this.samtoolsFlagStats = samtoolsFlagStats;
        return this;
    }

    public HsMetrics getHsMetrics() {
        return hsMetrics;
    }

    public Alignment setHsMetrics(HsMetrics hsMetrics) {
        this.hsMetrics = hsMetrics;
        return this;
    }
}
