package org.opencb.opencga.server.generator.openapi.models;

import java.util.Map;

public class Definition {

    private String type; // enum, object, array, string, integer, number, boolean, null
    private Map<String, FieldDefinition> properties;
    private String description;
    // List<String> required;

    // Getters y Setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, FieldDefinition> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, FieldDefinition> properties) {
        this.properties = properties;
    }

    public String getDescription() {
        return description;
    }

    public Definition setDescription(String description) {
        this.description = description;
        return this;
    }
}

