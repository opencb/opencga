package org.opencb.opencga.server.generator.models.openapi;

import java.util.Map;

public class Definition {
    private String type = "object";
    private Map<String, Property> properties;

    // Getters y Setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Property> properties) {
        this.properties = properties;
    }
}

