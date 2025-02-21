package org.opencb.opencga.core.models.workflow;

import java.util.List;

public class WorkflowAclUpdateParams {

    private List<String> workflowIds;
    private List<String> permissions;

    public WorkflowAclUpdateParams() {
    }

    public WorkflowAclUpdateParams(List<String> workflowIds, List<String> permissions) {
        this.workflowIds = workflowIds;
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowAclUpdateParams{");
        sb.append("workflowIds=").append(workflowIds);
        sb.append(", permissions=").append(permissions);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getWorkflowIds() {
        return workflowIds;
    }

    public WorkflowAclUpdateParams setWorkflowIds(List<String> workflowIds) {
        this.workflowIds = workflowIds;
        return this;
    }

    public List<String> getPermissions() {
        return permissions;
    }

    public WorkflowAclUpdateParams setPermissions(List<String> permissions) {
        this.permissions = permissions;
        return this;
    }
}
