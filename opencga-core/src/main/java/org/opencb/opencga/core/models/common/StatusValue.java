package org.opencb.opencga.core.models.common;

public class StatusValue {

    private String id;
    private String description;

    public StatusValue() {
    }

    public StatusValue(String id, String description) {
        this.id = id;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatusValue{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StatusValue setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StatusValue setDescription(String description) {
        this.description = description;
        return this;
    }
}
