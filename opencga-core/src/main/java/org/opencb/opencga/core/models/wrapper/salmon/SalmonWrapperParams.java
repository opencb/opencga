package org.opencb.opencga.core.models.wrapper.salmon;

import org.opencb.opencga.core.tools.ToolParams;

public class SalmonWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Salmon parameters.";

    private SalmonParams salmonParams;
    private String outdir;

    public SalmonWrapperParams() {
        this(new SalmonParams(), "");
    }

    public SalmonWrapperParams(SalmonParams salmonParams, String outdir) {
        this.salmonParams = salmonParams;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SalmonWrapperParams{");
        sb.append("salmonParams=").append(salmonParams);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public SalmonParams getSalmonParams() {
        return salmonParams;
    }

    public SalmonWrapperParams setMultiQcParams(SalmonParams salmonParams) {
        this.salmonParams = salmonParams;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SalmonWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
