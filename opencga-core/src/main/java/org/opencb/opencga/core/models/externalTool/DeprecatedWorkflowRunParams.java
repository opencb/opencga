package org.opencb.opencga.core.models.externalTool;

import java.util.Map;

@Deprecated
public class DeprecatedWorkflowRunParams {

    public static final String DESCRIPTION = "Workflow tool run parameters";

    private String id;
    private Integer version;
    private Map<String, String> params;

    public DeprecatedWorkflowRunParams() {
    }

    public DeprecatedWorkflowRunParams(String id, Integer version, Map<String, String> params) {
        this.id = id;
        this.version = version;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DeprecatedWorkflowRunParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public DeprecatedWorkflowRunParams setId(String id) {
        this.id = id;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public DeprecatedWorkflowRunParams setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public DeprecatedWorkflowRunParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
