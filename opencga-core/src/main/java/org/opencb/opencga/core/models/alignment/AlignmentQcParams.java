package org.opencb.opencga.core.models.alignment;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentQcParams extends ToolParams {
    public static final String STATS_SKIP_VALUE = "stats";
    public static final String FLAGSTATS_SKIP_VALUE = "flagstats";
    public static final String FASTQC_METRICS_SKIP_VALUE = "fastqc";

    public static final String DESCRIPTION = "Alignment quality control (QC) parameters. It computes: stats, flag stats and fastqc metrics."
            + " The BAM file ID is mandatory and in order to skip some metrics, use the following keywords (separated by commas): "
            + STATS_SKIP_VALUE + ", " + FLAGSTATS_SKIP_VALUE + ", " + FASTQC_METRICS_SKIP_VALUE;

    @DataField(id = "bamFile", description = FieldConstants.ALIGNMENT_QC_BAM_FILE_DESCRIPTION)
    private String bamFile;

    @DataField(id = "skip", description = FieldConstants.ALIGNMENT_QC_SKIP_DESCRIPTION)
    private String skip;

    @DataField(id = "overwrite", description = FieldConstants.ALIGNMENT_QC_OVERWRITE_DESCRIPTION)
    private boolean overwrite;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public AlignmentQcParams() {
    }

    public AlignmentQcParams(String bamFile, String skip, boolean overwrite, String outdir) {
        this.bamFile = bamFile;
        this.skip = skip;
        this.overwrite = overwrite;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentQcParams{");
        sb.append("bamFile='").append(bamFile).append('\'');
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
