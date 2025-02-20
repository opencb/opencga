package org.opencb.opencga.server.generator.openapi.models;

public class Response {
    private String description;
    private Schema schema = null;

    public Response() {
    }

    public String getDescription() {
        return description;
    }

    public Response setDescription(String description) {
        this.description = description;
        return this;
    }

    public Schema getSchema() {
        return schema;
    }

    public Response setSchema(Schema schema) {
        this.schema = schema;
        return this;
    }
}
