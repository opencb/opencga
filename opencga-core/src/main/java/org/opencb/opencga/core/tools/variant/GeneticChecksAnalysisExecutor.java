package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class GeneticChecksAnalysisExecutor extends OpenCgaToolExecutor {

    public enum GeneticCheck {
        SEX, RELATEDNESS, MENDELIAN_ERRORS
    }

    private String study;
    private List<String> samples;
    private GeneticCheck geneticCheck;

    public GeneticChecksAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public GeneticChecksAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public GeneticChecksAnalysisExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public GeneticCheck getGeneticCheck() {
        return geneticCheck;
    }

    public GeneticChecksAnalysisExecutor setGeneticCheck(GeneticCheck geneticCheck) {
        this.geneticCheck = geneticCheck;
        return this;
    }
}
