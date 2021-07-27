package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentQcParams extends ToolParams {
    public static String STATS_SKIP_VALUE = "stats";
    public static String FLAGSTATS_SKIP_VALUE = "flagstats";
    public static String FASTQC_METRICS_SKIP_VALUE = "fastqc";
    public static String HS_METRICS_SKIP_VALUE = "hsmetrics";

    public static final String DESCRIPTION = "Alignment quality control (QC) parameters. It computes: stats, flag stats, fastqc and"
            + " hybrid-selection metrics. The BAM file is mandatory ever but the BED fileand the dictionary files are only mandatory for"
            + " computing hybrid-selection (HS) metrics. In order to skip some metrics, use the following keywords (separated by commas): "
            + "stats, flagstats, fastqc and hsmetrics";

    private String bamFile;
    private String bedFile;
    private String dictFile;
    private String skip;
    private boolean overwrite;
    private String outdir;

    public AlignmentQcParams() {
    }

    public AlignmentQcParams(String bamFile, String bedFile, String dictFile, String skip, boolean overwrite, String outdir) {
        this.bamFile = bamFile;
        this.bedFile = bedFile;
        this.dictFile = dictFile;
        this.skip = skip;
        this.overwrite = overwrite;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentQcParams{");
        sb.append("bamFile='").append(bamFile).append('\'');
        sb.append(", bedFile='").append(bedFile).append('\'');
        sb.append(", dictFile='").append(dictFile).append('\'');
        sb.append(", skip='").append(skip).append('\'');
        sb.append(", overwrite=").append(overwrite);
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

    public String getSkip() {
        return skip;
    }

    public AlignmentQcParams setSkip(String skip) {
        this.skip = skip;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public AlignmentQcParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
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
