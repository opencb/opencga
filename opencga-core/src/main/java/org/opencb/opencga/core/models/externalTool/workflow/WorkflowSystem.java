package org.opencb.opencga.core.models.externalTool.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class WorkflowSystem {

    @DataField(id = "id", description = FieldConstants.WORKFLOW_SYSTEM_ID_DESCRIPTION)
    private SystemId id;

    @DataField(id = "version", description = FieldConstants.WORKFLOW_SYSTEM_VERSION_DESCRIPTION)
    private String version;

    public enum SystemId {
        NEXTFLOW
    }

    public WorkflowSystem() {
    }

    public WorkflowSystem(SystemId id, String version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowSystem{");
        sb.append("id=").append(id);
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public SystemId getId() {
        return id;
    }

    public WorkflowSystem setId(SystemId id) {
        this.id = id;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public WorkflowSystem setVersion(String version) {
        this.version = version;
        return this;
    }
}
