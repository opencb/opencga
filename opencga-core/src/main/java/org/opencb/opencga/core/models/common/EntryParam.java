package org.opencb.opencga.core.models.common;

public class EntryParam {

    private String id;
    private String uuid;

    public EntryParam() {
    }

    public EntryParam(String id, String uuid) {
        this.id = id;
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EntryParam{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public EntryParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public EntryParam setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
