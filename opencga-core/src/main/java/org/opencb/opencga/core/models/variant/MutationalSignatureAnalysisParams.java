package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class MutationalSignatureAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Mutational signature analysis params";
    private String sample;
    private String outdir;

    public MutationalSignatureAnalysisParams() {
    }

    public MutationalSignatureAnalysisParams(String sample, String outdir) {
        this.sample = sample;
        this.outdir = outdir;
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MutationalSignatureAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
