package org.opencb.opencga.core.models.nextflow;

import org.opencb.opencga.core.tools.ToolParams;

public class NextFlowRunParams extends ToolParams {

    public static final String DESCRIPTION = "NextFlow run parameters";

    private String id;
    private Integer version;

    public NextFlowRunParams() {
    }

    public NextFlowRunParams(String id, Integer version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NextFlowRunParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public NextFlowRunParams setId(String id) {
        this.id = id;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public NextFlowRunParams setVersion(Integer version) {
        this.version = version;
        return this;
    }
}
