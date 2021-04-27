package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentQcParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment quality control (QC) parameters. The BAM file is mandatory ever but the BED file" +
            " and the dictionary files are only mandatory for computing hybrid-selection (HS) metrics";

    private String bamFile;
    private String bedFile;
    private String dictFile;
    private boolean runSamtoolsStats;
    private boolean runSamtoolsFlagStats;
    private boolean runFastQc;
    private boolean runHsMetrics;
    private String outdir;

    public AlignmentQcParams() {
    }

    public AlignmentQcParams(String bamFile, String bedFile, String dictFile, boolean runSamtoolsStats, boolean runSamtoolsFlagStats,
                             boolean runFastQc, boolean runHsMetrics, String outdir) {
        this.bamFile = bamFile;
        this.bedFile = bedFile;
        this.dictFile = dictFile;
        this.runSamtoolsStats = runSamtoolsStats;
        this.runSamtoolsFlagStats = runSamtoolsFlagStats;
        this.runFastQc = runFastQc;
        this.runHsMetrics = runHsMetrics;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentQcParams{");
        sb.append("bamFile='").append(bamFile).append('\'');
        sb.append(", bedFile='").append(bedFile).append('\'');
        sb.append(", dictFile='").append(dictFile).append('\'');
        sb.append(", runSamtoolsStats=").append(runSamtoolsStats);
        sb.append(", runSamtoolsFlagStats=").append(runSamtoolsFlagStats);
        sb.append(", runFastQc=").append(runFastQc);
        sb.append(", runHsMetrics=").append(runHsMetrics);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getBamFile() {
        return bamFile;
    }

    public AlignmentQcParams setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getBedFile() {
        return bedFile;
    }

    public AlignmentQcParams setBedFile(String bedFile) {
        this.bedFile = bedFile;
        return this;
    }

    public String getDictFile() {
        return dictFile;
    }

    public AlignmentQcParams setDictFile(String dictFile) {
        this.dictFile = dictFile;
        return this;
    }

    public boolean isRunSamtoolsStats() {
        return runSamtoolsStats;
    }

    public AlignmentQcParams setRunSamtoolsStats(boolean runSamtoolsStats) {
        this.runSamtoolsStats = runSamtoolsStats;
        return this;
    }

    public boolean isRunSamtoolsFlagStats() {
        return runSamtoolsFlagStats;
    }

    public AlignmentQcParams setRunSamtoolsFlagStats(boolean runSamtoolsFlagStats) {
        this.runSamtoolsFlagStats = runSamtoolsFlagStats;
        return this;
    }

    public boolean isRunFastQc() {
        return runFastQc;
    }

    public AlignmentQcParams setRunFastQc(boolean runFastQc) {
        this.runFastQc = runFastQc;
        return this;
    }

    public boolean isRunHsMetrics() {
        return runHsMetrics;
    }

    public AlignmentQcParams setRunHsMetrics(boolean runHsMetrics) {
        this.runHsMetrics = runHsMetrics;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public AlignmentQcParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
