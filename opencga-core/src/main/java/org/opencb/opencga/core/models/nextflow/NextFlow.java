package org.opencb.opencga.core.models.nextflow;

public class NextFlow {

    private String id;
    private int version;
    private String script;

    public NextFlow() {
    }

    public NextFlow(String id, int version, String script) {
        this.id = id;
        this.version = version;
        this.script = script;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NextFlow{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", script='").append(script).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public NextFlow setId(String id) {
        this.id = id;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public NextFlow setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getScript() {
        return script;
    }

    public NextFlow setScript(String script) {
        this.script = script;
        return this;
    }
}
