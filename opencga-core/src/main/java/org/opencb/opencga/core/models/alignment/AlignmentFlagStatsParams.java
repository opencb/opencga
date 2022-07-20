package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class AlignmentFlagStatsParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment flag stats params";

    @DataField(description = ParamConstants.ALIGNMENT_FLAG_STATS_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.ALIGNMENT_FLAG_STATS_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;

    public AlignmentFlagStatsParams() {
    }

    public AlignmentFlagStatsParams(String file, String outdir) {
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

    public AlignmentFlagStatsParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public AlignmentFlagStatsParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
