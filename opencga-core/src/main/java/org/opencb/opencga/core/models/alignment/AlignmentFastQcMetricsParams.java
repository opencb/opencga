package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentFastQcMetricsParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment FastQC metrics params";

    private String file;
    private String outdir;

    public AlignmentFastQcMetricsParams() {
    }

    public AlignmentFastQcMetricsParams(String file, String outdir) {
        this.file = file;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentFlagStatsParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public AlignmentFastQcMetricsParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public AlignmentFastQcMetricsParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
