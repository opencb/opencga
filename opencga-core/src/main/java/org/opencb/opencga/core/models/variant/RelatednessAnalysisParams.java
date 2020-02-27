package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class RelatednessAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Relatedness analysis params";
    private List<String> samples;
    private List<String> families;
    private String method;
    private String population;
    private String outdir;

    public RelatednessAnalysisParams() {
    }

    public RelatednessAnalysisParams(List<String> samples, List<String> families, String method, String population, String outdir) {
        this.samples = samples;
        this.families = families;
        this.method = method;
        this.population = population;
        this.outdir = outdir;
    }

    public List<String> getSamples() {
        return samples;
    }

    public RelatednessAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public List<String> getFamilies() {
        return families;
    }

    public RelatednessAnalysisParams setFamilies(List<String> families) {
        this.families = families;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public RelatednessAnalysisParams setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public RelatednessAnalysisParams setPopulation(String population) {
        this.population = population;
        return this;
    }

    public RelatednessAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
