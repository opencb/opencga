package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class GeneticChecksAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Genetic checks analysis params";
    private List<String> samples;
    private String outdir;

    public GeneticChecksAnalysisParams() {
    }

    public GeneticChecksAnalysisParams(List<String> samples, String outdir) {
        this.samples = samples;
        this.outdir = outdir;
    }

    public List<String> getSamples() {
        return samples;
    }

    public GeneticChecksAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public GeneticChecksAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
