package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantAggregateFamilyParams extends ToolParams {
    public static final String DESCRIPTION = "Variant aggregate family params.";
    private List<String> samples;
    private boolean resume;

    public VariantAggregateFamilyParams() {
    }

    public VariantAggregateFamilyParams(List<String> samples, boolean resume) {
        this.samples = samples;
        this.resume = resume;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantAggregateFamilyParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public VariantAggregateFamilyParams setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }
}
