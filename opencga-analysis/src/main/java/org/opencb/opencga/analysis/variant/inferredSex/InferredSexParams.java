package org.opencb.opencga.analysis.variant.inferredSex;

import org.opencb.opencga.core.tools.ToolParams;

public class InferredSexParams extends ToolParams {

    private String individual;

    public InferredSexParams() {
    }

    public InferredSexParams(String individual) {
        this.individual = individual;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InferredSexParams{");
        sb.append("individualId='").append(individual).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getIndividual() {
        return individual;
    }

    public InferredSexParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }
}
