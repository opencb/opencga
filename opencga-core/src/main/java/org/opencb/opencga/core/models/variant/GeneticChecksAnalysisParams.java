package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class GeneticChecksAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Genetic checks analysis params";
    private List<String> samples;
    private List<String> families;
    private String relatednessMethod;
    private String population;
    private String outdir;

    public GeneticChecksAnalysisParams() {
    }

    public GeneticChecksAnalysisParams(List<String> samples, List<String> families, String relatednessMethod, String population,
                                       String outdir) {
        this.samples = samples;
        this.families = families;
        this.relatednessMethod = relatednessMethod;
        this.population = population;
        this.outdir = outdir;
    }

    public List<String> getSamples() {
        return samples;
    }

    public GeneticChecksAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public List<String> getFamilies() {
        return families;
    }

    public GeneticChecksAnalysisParams setFamilies(List<String> families) {
        this.families = families;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public GeneticChecksAnalysisParams setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public GeneticChecksAnalysisParams setPopulation(String population) {
        this.population = population;
        return this;
    }

    public GeneticChecksAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
