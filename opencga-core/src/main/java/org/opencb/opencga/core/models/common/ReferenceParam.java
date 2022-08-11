package org.opencb.opencga.core.models.common;

public class ReferenceParam {

    private String id;
    private String uuid;

    public ReferenceParam() {
        this("", "");
    }

    public ReferenceParam(String id, String uuid) {
        this.id = id;
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReferenceParam{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ReferenceParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public ReferenceParam setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
