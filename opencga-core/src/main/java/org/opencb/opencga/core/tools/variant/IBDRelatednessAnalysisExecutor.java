package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;

public abstract class IBDRelatednessAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private List<String> samples;
    private String minorAlleleFreq;

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

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public IBDRelatednessAnalysisExecutor setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }
}
