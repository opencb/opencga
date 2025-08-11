package org.opencb.opencga.core.models.wrapper;

import org.opencb.opencga.core.tools.ToolParams;

public class Hisat2WrapperParams extends ToolParams {
    public static final String DESCRIPTION = "HISAT2 parameters.";

    private Hisat2Params hisat2Params;
    private String outdir;

    public Hisat2WrapperParams() {
        this.hisat2Params = new Hisat2Params();
    }
    public Hisat2WrapperParams(Hisat2Params hisat2Params, String outdir) {
        this.hisat2Params = hisat2Params;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Hisat2WrapperParams{");
        sb.append("hisat2Params=").append(hisat2Params);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Hisat2Params getHisat2Params() {
        return hisat2Params;
    }

    public Hisat2WrapperParams setHisat2Params(Hisat2Params hisat2Params) {
        this.hisat2Params = hisat2Params;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public Hisat2WrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
