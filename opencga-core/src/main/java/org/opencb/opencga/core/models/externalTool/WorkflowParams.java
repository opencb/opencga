package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class WorkflowParams extends ToolParams implements ExternalToolParams {

    public static final String DESCRIPTION = "Workflow tool run parameters";

    @DataField(id = "id", description = "User tool identifier.")
    private String id;

    @DataField(id = "version", description = "User tool version. If not provided, the latest version will be used.")
    private Integer version;

    @DataField(id = "params", description = "Key-value pairs of parameters to be used inside the workflow.")
    private Map<String, String> params;

    public WorkflowParams() {
    }

    public WorkflowParams(String id, Integer version, Map<String, String> params) {
        this.id = id;
        this.version = version;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public WorkflowParams setId(String id) {
        this.id = id;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public WorkflowParams setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public WorkflowParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
