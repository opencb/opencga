package org.opencb.opencga.analysis.variant.mendelianError;

import org.opencb.opencga.core.tools.ToolParams;

public class MendelianErrorParams extends ToolParams {

    private String family;
    private String individual;
    private String sample;

    public MendelianErrorParams() {
    }

    public MendelianErrorParams(String family, String individual, String sample) {
        this.family = family;
        this.individual = individual;
        this.sample = sample;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MendelianErrorParams{");
        sb.append("familyId='").append(family).append('\'');
        sb.append(", individualId='").append(individual).append('\'');
        sb.append(", sampleId='").append(sample).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamily() {
        return family;
    }

    public MendelianErrorParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public MendelianErrorParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public MendelianErrorParams setSample(String sample) {
        this.sample = sample;
        return this;
    }
}
