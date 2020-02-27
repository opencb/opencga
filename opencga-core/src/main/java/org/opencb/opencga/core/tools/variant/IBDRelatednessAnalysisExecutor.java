package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class IBDRelatednessAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private List<String> samples;
    private String population;

    public IBDRelatednessAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public IBDRelatednessAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public IBDRelatednessAnalysisExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getPopulation() {
        return population;
    }

    public IBDRelatednessAnalysisExecutor setPopulation(String population) {
        this.population = population;
        return this;
    }
}
