package org.opencb.opencga.core.models.wrapper.multiqc;

import org.opencb.opencga.core.tools.ToolParams;

public class MultiQcWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "MultiQC parameters.";

    private MultiQcParams multiQcParams;
    private String outdir;

    public MultiQcWrapperParams() {
        super();
        this.multiQcParams = new MultiQcParams();
    }

    public MultiQcWrapperParams(MultiQcParams multiQcParams, String outdir) {
        this.multiQcParams = multiQcParams;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MultiQcWrapperParams{");
        sb.append("multiQcParams=").append(multiQcParams);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public MultiQcParams getMultiQcParams() {
        return multiQcParams;
    }

    public MultiQcWrapperParams setMultiQcParams(MultiQcParams multiQcParams) {
        this.multiQcParams = multiQcParams;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MultiQcWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
