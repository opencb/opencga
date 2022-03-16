package org.opencb.opencga.core.models.common;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.Map;

public class AdditionalInfo {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;
    @DataField(id = "name", indexed = true,
            description = FieldConstants.GENERIC_NAME)
    private String name;

    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "type", indexed = true,
            description = FieldConstants.ADDITIONAL_INFO_TYPE)
    private String type;

    @DataField(id = "attributes", indexed = true,
            description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
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
