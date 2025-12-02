package org.opencb.opencga.server.generator.openapi.models;

import java.util.List;

/**
 * Parameter class representing an OpenAPI parameter object.
 *
 * @see <a href=https://swagger.io/specification/v2/#parameter-object>Parameter Object</a>
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/2.0.md#parameter-object">Parameter Object</a>
 */
public class Parameter {
    private String name;
    private String in; // Accepted values: "query", "path", "header" "formData", "body"
    private String description;
    private boolean required;

    // Other parameter
    private String type;
    private String format;
    private String defaultValue; // Default value

    private List<String> enumValues; // Enum values

    // Body Parameter
    private Schema schema = null;

    public Parameter() {
    }

    // Getters y Setters

    public String getName() {
        return name;
    }

    public Parameter setName(String name) {
        this.name = name;
        return this;
    }

    public String getIn() {
        return in;
    }

    public Parameter setIn(String in) {
        this.in = in;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Parameter setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isRequired() {
        return required;
    }

    public Parameter setRequired(boolean required) {
        this.required = required;
        return this;
    }

    public String getType() {
        return type;
    }

    public Parameter setType(String type) {
        this.type = type;
        return this;
    }

    public String getFormat() {
        return format;
    }

    public Parameter setFormat(String format) {
        this.format = format;
        return this;
    }

    public String getDefault() {
        return defaultValue;
    }

    public Parameter setDefault(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public List<String> getEnum() {
        return enumValues;
    }

    public Parameter setEnum(List<String> enumValues) {
        this.enumValues = enumValues;
        return this;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

}

