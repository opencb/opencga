package org.opencb.opencga.core.tools;

public class ToolDependency {

    private String id;
    private String version;
    private String commit;

    public ToolDependency() {
    }

    public ToolDependency(String id, String version, String commit) {
        this.id = id;
        this.version = version;
        this.commit = commit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ToolDependency{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", commit='").append(commit).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ToolDependency setId(String id) {
        this.id = id;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public ToolDependency setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getCommit() {
        return commit;
    }

    public ToolDependency setCommit(String commit) {
        this.commit = commit;
        return this;
    }
}
