package org.opencb.opencga.core.models.workflow;

public class WorkflowImportParams {

    private String source;
    private String id;

    public WorkflowImportParams() {
    }

    public WorkflowImportParams(String source, String id) {
        this.source = source;
        this.id = id;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowImportParams{");
        sb.append("source='").append(source).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSource() {
        return source;
    }

    public WorkflowImportParams setSource(String source) {
        this.source = source;
        return this;
    }

    public String getId() {
        return id;
    }

    public WorkflowImportParams setId(String id) {
        this.id = id;
        return this;
    }
}
