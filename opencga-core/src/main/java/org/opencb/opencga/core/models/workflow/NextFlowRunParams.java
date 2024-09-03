package org.opencb.opencga.core.models.workflow;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class NextFlowRunParams extends ToolParams {

    public static final String DESCRIPTION = "NextFlow run parameters";

    private String id;
    private Integer version;
    private Map<String, String> params;

    public NextFlowRunParams() {
    }

    public NextFlowRunParams(String id, Integer version, Map<String, String> params) {
        this.id = id;
        this.version = version;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NextFlowRunParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params='").append(params).append('\'');
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

    public Map<String, String> getParams() {
        return params;
    }

    public NextFlowRunParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
