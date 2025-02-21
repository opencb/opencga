package org.opencb.opencga.core.models.job;

public class ToolInfoExecutor {

    private String id;
    private String version;

    public ToolInfoExecutor() {
    }

    public ToolInfoExecutor(String id, String version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ToolInfoExecutor{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ToolInfoExecutor setId(String id) {
        this.id = id;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public ToolInfoExecutor setVersion(String version) {
        this.version = version;
        return this;
    }
}
