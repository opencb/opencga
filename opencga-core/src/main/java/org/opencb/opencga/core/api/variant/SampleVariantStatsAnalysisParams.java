package org.opencb.opencga.core.api.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class SampleVariantStatsAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Sample variant stats params";
    private List<String> sample;
    private String family;
    private boolean index;
    private String sampleAnnotation;
    private String outdir;

    public SampleVariantStatsAnalysisParams() {
    }
    public SampleVariantStatsAnalysisParams(List<String> sample, String family,
                                            boolean index, String sampleAnnotation, String outdir) {
        this.sample = sample;
        this.family = family;
        this.index = index;
        this.sampleAnnotation = sampleAnnotation;
        this.outdir = outdir;
    }

    public List<String> getSample() {
        return sample;
    }

    public SampleVariantStatsAnalysisParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public SampleVariantStatsAnalysisParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public SampleVariantStatsAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }

    public String getSampleAnnotation() {
        return sampleAnnotation;
    }

    public SampleVariantStatsAnalysisParams setSampleAnnotation(String sampleAnnotation) {
        this.sampleAnnotation = sampleAnnotation;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public SampleVariantStatsAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
