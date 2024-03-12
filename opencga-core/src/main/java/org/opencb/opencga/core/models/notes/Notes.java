package org.opencb.opencga.core.models.notes;

import org.opencb.opencga.core.models.PrivateFields;

import java.util.List;

public class Notes extends PrivateFields {

    // TODO: Indexes - id, version  <---
    private String id;
    private String uuid;
    private int version;
    private String userId;
    private List<String> tags;
    private String creationDate;
    private String modificationDate;
    private Type valueType;
    private Object value;

    public enum Type {
        OBJECT,
        ARRAY,
        STRING,
        INTEGER,
        DOUBLE
    }

    public Notes() {
    }

    public Notes(String id, String uuid, int version, String userId, List<String> tags, String creationDate, String modificationDate,
                 Type valueType, Object value) {
        this.id = id;
        this.uuid = uuid;
        this.version = version;
        this.userId = userId;
        this.tags = tags;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.valueType = valueType;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Notes{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", version=").append(version);
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", valueType=").append(valueType);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Notes setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Notes setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Notes setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public Notes setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Notes setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Notes setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Notes setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Type getValueType() {
        return valueType;
    }

    public Notes setValueType(Type valueType) {
        this.valueType = valueType;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public Notes setValue(Object value) {
        this.value = value;
        return this;
    }
}
