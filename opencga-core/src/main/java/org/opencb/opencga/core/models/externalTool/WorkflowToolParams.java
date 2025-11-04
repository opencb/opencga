package org.opencb.opencga.core.models.externalTool;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class WorkflowToolParams extends ToolParams {

    private Map<String, String> params;

    public WorkflowToolParams() {
    }

    public WorkflowToolParams(Map<String, String> params) {
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

    public WorkflowToolParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
