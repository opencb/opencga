package org.opencb.opencga.core.models.sample;

public class SampleReferenceParam {

    private String id;
    private String uuid;

    public SampleReferenceParam() {
    }

    public SampleReferenceParam(String id, String uuid) {
        this.id = id;
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleReferenceParam{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleReferenceParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public SampleReferenceParam setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
