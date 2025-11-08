package org.opencb.opencga.core.models.externalTool;

public class WorkflowToolParams extends ExternalToolParams<WorkflowParams> {

    public WorkflowToolParams() {
    }

    public WorkflowToolParams(String id, Integer version, WorkflowParams params) {
        super(id, version, params);
    }

}
