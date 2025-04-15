package org.opencb.opencga.core.models.externalTool;

public class ExternalToolImportParams {

    private String source;
    private String id;

    public ExternalToolImportParams() {
    }

    public ExternalToolImportParams(String source, String id) {
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

    public ExternalToolImportParams setSource(String source) {
        this.source = source;
        return this;
    }

    public String getId() {
        return id;
    }

    public ExternalToolImportParams setId(String id) {
        this.id = id;
        return this;
    }
}
