package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantSampleIndexParams extends ToolParams {

    public static final String DESCRIPTION = "Variant sample index params";
    private List<String> sample;
    private boolean buildIndex;
    private boolean annotate;

    public VariantSampleIndexParams() {
    }

    public VariantSampleIndexParams(List<String> sample, boolean buildIndex, boolean annotate) {
        this.sample = sample;
        this.buildIndex = buildIndex;
        this.annotate = annotate;
    }

    public List<String> getSample() {
        return sample;
    }

    public VariantSampleIndexParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isBuildIndex() {
        return buildIndex;
    }

    public VariantSampleIndexParams setBuildIndex(boolean buildIndex) {
        this.buildIndex = buildIndex;
        return this;
    }

    public boolean isAnnotate() {
        return annotate;
    }

    public VariantSampleIndexParams setAnnotate(boolean annotate) {
        this.annotate = annotate;
        return this;
    }
}
