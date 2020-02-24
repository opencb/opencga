package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class RelatednessAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Relatedness analysis params";
    private List<String> samples;
    private String outdir;

    public RelatednessAnalysisParams() {
    }

    public RelatednessAnalysisParams(List<String> samples, String outdir) {
        this.samples = samples;
        this.outdir = outdir;
    }

    public List<String> getSamples() {
        return samples;
    }

    public RelatednessAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public RelatednessAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
