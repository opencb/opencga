package org.opencb.opencga.core.api.variant;

import java.util.Arrays;
import java.util.List;

public class SampleVariantFilterParams extends BasicVariantQueryParams {

    public static final String DESCRIPTION = "Sample variant filter params";
    private List<String> genotypes = Arrays.asList("0/1", "1/1");
    private List<String> sample;
    private boolean samplesInAllVariants = false;
    private int maxVariants = 50;

    public List<String> getGenotypes() {
        return genotypes;
    }

    public SampleVariantFilterParams setGenotypes(List<String> genotypes) {
        this.genotypes = genotypes;
        return this;
    }

    public List<String> getSample() {
        return sample;
    }

    public SampleVariantFilterParams setSample(List<String> sample) {
        this.sample = sample;
        return this;
    }

    public boolean isSamplesInAllVariants() {
        return samplesInAllVariants;
    }

    public SampleVariantFilterParams setSamplesInAllVariants(boolean samplesInAllVariants) {
        this.samplesInAllVariants = samplesInAllVariants;
        return this;
    }

    public int getMaxVariants() {
        return maxVariants;
    }

    public SampleVariantFilterParams setMaxVariants(int maxVariants) {
        this.maxVariants = maxVariants;
        return this;
    }
}
