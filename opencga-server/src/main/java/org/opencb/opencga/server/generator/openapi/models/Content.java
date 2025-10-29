package org.opencb.opencga.server.generator.openapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Content {
    private Schema schema;

    public Content() {}

    public Schema getSchema() {
        return schema;
    }

    public Content setSchema(Schema schema) {
        this.schema = schema;
        return this;
    }
}