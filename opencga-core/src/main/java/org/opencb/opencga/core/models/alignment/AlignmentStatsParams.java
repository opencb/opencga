package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentStatsParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment stats params";

    private String file;
    private String outdir;

    public AlignmentStatsParams() {
    }

    public AlignmentStatsParams(String file, String outdir) {
        this.file = file;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentStatsParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public AlignmentStatsParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public AlignmentStatsParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
