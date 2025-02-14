package org.opencb.opencga.server.generator.openapi.models;

public class Parameter {
    private String name;
    private String in; // Ejemplo: "query", "path", "header"
    private String description;
    private boolean required;
    private String type="";
    private String format="";
    private String defaultValue="";

    public Parameter() {
    }

    public Parameter(String name, String in, String description, boolean required, String type, String format) {
        this.name = name;
        this.in = in;
        this.description = description;
        this.required = required;
        this.type = type;
        this.format = format;
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

    public String getDefaultValue() {
        return defaultValue;
    }

    public Parameter setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}

