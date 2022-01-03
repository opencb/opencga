package org.opencb.opencga.analysis.variant.inferredSex;

import org.opencb.opencga.core.tools.ToolParams;

public class InferredSexParams extends ToolParams {

    private String individualId;

    public InferredSexParams() {
    }

    public InferredSexParams(String individualId) {
        this.individualId = individualId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InferredSexParams{");
        sb.append("individualId='").append(individualId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getIndividualId() {
        return individualId;
    }

    public InferredSexParams setIndividualId(String individualId) {
        this.individualId = individualId;
        return this;
    }
}
