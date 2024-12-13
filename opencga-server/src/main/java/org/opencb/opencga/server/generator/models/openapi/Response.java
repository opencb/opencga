package org.opencb.opencga.server.generator.models.openapi;

import java.util.HashMap;
import java.util.Map;

public class Response {
    private String description;
    private Map<String,String> schema;

    public Response(String description, Map<String, String> schema) {
        this.description = description;
        this.schema = schema;
    }

    public Response() {
        schema = new HashMap<>();
    }

    // Getters y Setters
    public String getDescription() {
        return description;
    }

    public Response setDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, String> getSchema() {
        return schema;
    }

    public Response setSchema(Map<String, String> schema) {
        this.schema = schema;
        return this;
    }
}
