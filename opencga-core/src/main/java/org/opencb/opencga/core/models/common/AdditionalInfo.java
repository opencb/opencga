package org.opencb.opencga.core.models.common;

import java.util.Map;

public class AdditionalInfo {

    private String id;
    private String name;
    private String description;
    private String type;
    private Map<String, Object> attributes;

    public AdditionalInfo() {
    }

    public AdditionalInfo(String id, String name, String description, String type, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AdditionalInfo{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AdditionalInfo setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public AdditionalInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public AdditionalInfo setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getType() {
        return type;
    }

    public AdditionalInfo setType(String type) {
        this.type = type;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public AdditionalInfo setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
