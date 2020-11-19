package org.opencb.opencga.core.models.common;

public class StatusParams {

    private String id;

    public StatusParams() {
    }

    public StatusParams(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatusParams{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StatusParams setId(String id) {
        this.id = id;
        return this;
    }
}
