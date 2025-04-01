package org.opencb.opencga.server.generator.openapi.models;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Response {
    private String description;
    private Schema schema; // para compatibilidad
    private Map<String, Content> content; // OpenAPI 3

    public Response() {}

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

    public Map<String, Content> getContent() {
        return content;
    }

    public Response setContent(Map<String, Content> content) {
        this.content = content;
        return this;
    }
}
