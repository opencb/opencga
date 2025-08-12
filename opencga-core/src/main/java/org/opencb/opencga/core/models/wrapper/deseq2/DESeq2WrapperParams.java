package org.opencb.opencga.core.models.wrapper.deseq2;

import org.opencb.opencga.core.tools.ToolParams;

public class DESeq2WrapperParams extends ToolParams {
    public static final String DESCRIPTION = "DESeq2 parameters.";

    private DESeq2Params deSeq2Params2Params;
    private String outdir;

    public DESeq2WrapperParams() {
        this(new DESeq2Params(), "");
    }

    public DESeq2WrapperParams(DESeq2Params deSeq2Params2Params, String outdir) {
        this.deSeq2Params2Params = deSeq2Params2Params;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DESeq2WrapperParams{");
        sb.append("deSeq2Params2Params=").append(deSeq2Params2Params);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public DESeq2Params getDESeq2Params2Params() {
        return deSeq2Params2Params;
    }

    public DESeq2WrapperParams setDESeq2Params2Params(DESeq2Params deSeq2Params2Params) {
        this.deSeq2Params2Params = deSeq2Params2Params;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public DESeq2WrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
