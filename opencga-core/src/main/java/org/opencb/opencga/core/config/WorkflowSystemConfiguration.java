package org.opencb.opencga.core.config;

public class WorkflowSystemConfiguration {

    private String id;
    private String version;

    public WorkflowSystemConfiguration() {
    }

    public WorkflowSystemConfiguration(String id, String version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowSystemConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public WorkflowSystemConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public WorkflowSystemConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }
}
