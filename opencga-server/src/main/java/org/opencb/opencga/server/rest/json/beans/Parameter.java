package org.opencb.opencga.server.rest.json.beans;

import java.util.List;

public class Parameter {

    private String name;
    private String param;
    private String type;
    private String typeClass;
    private boolean required;
    private String defaultValue;
    private String description;
    private List<Parameter> data;
    private String allowedValues;
    private int position;

    public boolean isRequired() {
        return required;
    }

    public Parameter setRequired(boolean required) {
        this.required = required;
        return this;
    }

    public String getAllowedValues() {
        return allowedValues;
    }

    public Parameter setAllowedValues(String allowedValues) {
        this.allowedValues = allowedValues;
        return this;
    }

    public int getPosition() {
        return position;
    }

    public Parameter setPosition(int position) {
        this.position = position;
        return this;
    }

    public String getName() {
        return name;
    }

    public Parameter setName(String name) {
        this.name = name;
        return this;
    }

    public String getParam() {
        return param;
    }

    public Parameter setParam(String param) {
        this.param = param;
        return this;
    }

    public String getType() {
        return type;
    }

    public Parameter setType(String type) {
        this.type = type;
        return this;
    }

    public String getTypeClass() {
        return typeClass;
    }

    public Parameter setTypeClass(String typeClass) {
        this.typeClass = typeClass;
        return this;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Parameter setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Parameter setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Parameter> getData() {
        return data;
    }

    public Parameter setData(List<Parameter> data) {
        this.data = data;
        return this;
    }

    @Override
    public String toString() {
        return "Parameter{" +
                "name='" + name + '\'' +
                ", param='" + param + '\'' +
                ", type='" + type + '\'' +
                ", typeClass='" + typeClass + '\'' +
                ", required='" + required + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                ", description='" + description + '\'' +
                ", data=" + data +
                "}\n";
    }
}
