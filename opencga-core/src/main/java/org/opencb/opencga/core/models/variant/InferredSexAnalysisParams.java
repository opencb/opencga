package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class InferredSexAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Inferred sex analysis params";
    private String individual;
    private String sample;
    private String outdir;

    public InferredSexAnalysisParams() {
    }

    public InferredSexAnalysisParams(String individual, String sample, String outdir) {
        this.individual = individual;
        this.sample = sample;
        this.outdir = outdir;
    }

    public String getIndividual() {
        return individual;
    }

    public InferredSexAnalysisParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public InferredSexAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public InferredSexAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
