package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.models.variant.GeneticChecksReport;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class GeneticChecksAnalysisExecutor extends OpenCgaToolExecutor {

    public enum GeneticCheck {
        INFERRED_SEX, RELATEDNESS, MENDELIAN_ERRORS
    }

    private String study;
    private String family;
    private List<String> samples;
    private GeneticCheck geneticCheck;
    private String minorAlleleFreq;
    private String relatednessMethod;

    private GeneticChecksReport output;

    public GeneticChecksAnalysisExecutor() {
        output = new GeneticChecksReport();
    }

    public String getStudy() {
        return study;
    }

    public GeneticChecksAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public GeneticChecksAnalysisExecutor setFamily(String family) {
        this.family = family;
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

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public GeneticChecksAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getRelatednessMethod() {
        return relatednessMethod;
    }

    public GeneticChecksAnalysisExecutor setRelatednessMethod(String relatednessMethod) {
        this.relatednessMethod = relatednessMethod;
        return this;
    }

    public GeneticChecksReport getOutput() {
        return output;
    }

    public GeneticChecksAnalysisExecutor setOutput(GeneticChecksReport output) {
        this.output = output;
        return this;
    }
}
