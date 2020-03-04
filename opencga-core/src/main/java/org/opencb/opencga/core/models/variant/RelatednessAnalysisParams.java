package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class RelatednessAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Relatedness analysis params";
    private List<String> samples;
    private String minorAlleleFreq;
    private String method;
    private String outdir;

    public RelatednessAnalysisParams() {
    }

    public RelatednessAnalysisParams(List<String> samples, String minorAlleleFreq, String method, String outdir) {
        this.samples = samples;
        this.minorAlleleFreq = minorAlleleFreq;
        this.method = method;
        this.outdir = outdir;
    }

    public List<String> getSamples() {
        return samples;
    }

    public RelatednessAnalysisParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    public String getMinorAlleleFreq() {
        return minorAlleleFreq;
    }

    public RelatednessAnalysisParams setMinorAlleleFreq(String minorAlleleFreq) {
        this.minorAlleleFreq = minorAlleleFreq;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public RelatednessAnalysisParams setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public RelatednessAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
