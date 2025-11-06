package org.opencb.opencga.core.models.externalTool;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class WorkflowParams extends ToolParams {

    public static final String DESCRIPTION = "Workflow tool run parameters";

    private Map<String, String> params;

    public WorkflowParams() {
    }

    public WorkflowParams(Map<String, String> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowToolParams{");
        sb.append("params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, String> getParams() {
        return params;
    }

    public WorkflowParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
