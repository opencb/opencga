package org.opencb.opencga.core.models.externalTool;

public class WorkflowRepositoryParams {

    private String id;
    private String version;

    public WorkflowRepositoryParams() {
    }

    public WorkflowRepositoryParams(String id) {
        this.id = id;
    }

    public WorkflowRepositoryParams(String id, String version) {
        this.id = id;
        this.version = version;
    }

    public WorkflowRepository toWorkflowRepository() {
        return new WorkflowRepository(id, version, "", "");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowRepositoryParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public WorkflowRepositoryParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public WorkflowRepositoryParams setVersion(String version) {
        this.version = version;
        return this;
    }
}
