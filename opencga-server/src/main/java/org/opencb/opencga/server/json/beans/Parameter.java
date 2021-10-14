package org.opencb.opencga.server.json.beans;

import java.util.ArrayList;
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
    private boolean complex;
    private String genericType;

    @Deprecated
    public List<Parameter> getAvailableData() {
        List<Parameter> res = new ArrayList<>();
        if (data != null && data.size() > 0) {
            for (Parameter p : data) {
            }
        }
        return res;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Parameter{");
        sb.append("name='").append(name).append('\'');
        sb.append(", param='").append(param).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", typeClass='").append(typeClass).append('\'');
        sb.append(", required=").append(required);
        sb.append(", defaultValue='").append(defaultValue).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", data=").append(data);
        sb.append(", allowedValues='").append(allowedValues).append('\'');
        sb.append(", position=").append(position);
        sb.append(", complex=").append(complex);
        sb.append(", genericType='").append(genericType).append('\'');
        sb.append('}');
        return sb.toString();
    }

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

    public boolean isComplex() {
        return complex;
    }

    public Parameter setComplex(boolean complex) {
        this.complex = complex;
        return this;
    }

    public String getGenericType() {
        return genericType;
    }

    public Parameter setGenericType(String genericType) {
        this.genericType = genericType;
        return this;
    }

    public boolean isStringList() {
        return isList() && "java.util.List<java.lang.String>".equals(getGenericType());
    }

    public boolean isList() {

        return isComplex() && "List".equals(getType());
    }

    public boolean isAvailableType() {
        return (!isComplex() || isStringList());
    }
}
