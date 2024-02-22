package org.opencb.opencga.server.generator.models;

import org.opencb.opencga.core.tools.annotations.RestParamType;

import java.util.List;

public class RestParameter {

    /**
     * Name of the param.
     */
    private String name;

    /**
     * Param type. Either query, path or body.
     */
    private RestParamType param;

    /**
     * Parent param name. Useful for inner params.
     */
    private String parentName;

    /**
     * High level param type.
     * <p>
     * - enum
     * - string
     * - boolean
     * - object
     * - class.getSimpleName().toLowerCase()
     */
    private String type;

    /**
     * Java class of this value.
     */
    private String typeClass;
    private boolean required;
    private String defaultValue;
    private String description;

    /**
     * Inner fields for object type models.
     */
    private List<RestParameter> data;
    private String allowedValues;
    private boolean complex;

    /**
     * Canonical type with generics.
     * e.g.
     * - java.util.List<java.lang.String>
     * - java.util.Map<java.lang.String, java.lang.Object>
     */
    private String genericType;

    /**
     * Flattened param. This param has a parent param.
     */
    private boolean innerParam;

    public boolean isStringList() {
        return isList() && "java.util.List<java.lang.String>".equals(getGenericType());
    }

    public boolean isList() {
        return isComplex() && "List".equals(getType());
    }

    public boolean isAvailableType() {
        return ((!isComplex()) || isStringList());
    }

    public boolean isCollection() {
        return isComplex() && ("List".equals(getType()) || "Map".equals(getType()));
    }

    @Override
    public String toString() {
        return "RestParameter{" +
                "name='" + name + '\'' +
                ", param='" + param + '\'' +
                ", parentName='" + parentName + '\'' +
                ", type='" + type + '\'' +
                ", typeClass='" + typeClass + '\'' +
                ", required=" + required +
                ", defaultValue='" + defaultValue + '\'' +
                ", description='" + description + '\'' +
                ", data=" + data +
                ", allowedValues='" + allowedValues + '\'' +
                ", complex=" + complex +
                ", genericType='" + genericType + '\'' +
                ", innerParam=" + innerParam +
                "}";
    }

    public String getName() {
        return name;
    }

    public RestParameter setName(String name) {
        this.name = name;
        return this;
    }

    public RestParamType getParam() {
        return param;
    }

    public RestParameter setParam(RestParamType param) {
        this.param = param;
        return this;
    }

    public String getParentName() {
        return parentName;
    }

    public RestParameter setParentName(String parentName) {
        this.parentName = parentName;
        return this;
    }

    public String getType() {
        return type;
    }

    public RestParameter setType(String type) {
        this.type = type;
        return this;
    }

    public String getTypeClass() {
        return typeClass;
    }

    public RestParameter setTypeClass(String typeClass) {
        this.typeClass = typeClass;
        return this;
    }

    public boolean isRequired() {
        return required;
    }

    public RestParameter setRequired(boolean required) {
        this.required = required;
        return this;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public RestParameter setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public RestParameter setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<RestParameter> getData() {
        return data;
    }

    public RestParameter setData(List<RestParameter> data) {
        this.data = data;
        return this;
    }

    public String getAllowedValues() {
        return allowedValues;
    }

    public RestParameter setAllowedValues(String allowedValues) {
        this.allowedValues = allowedValues;
        return this;
    }

    public boolean isComplex() {
        return complex;
    }

    public RestParameter setComplex(boolean complex) {
        this.complex = complex;
        return this;
    }

    public String getGenericType() {
        return genericType;
    }

    public RestParameter setGenericType(String genericType) {
        this.genericType = genericType;
        return this;
    }

    public boolean isInnerParam() {
        return innerParam;
    }

    public RestParameter setInnerParam(boolean innerParam) {
        this.innerParam = innerParam;
        return this;
    }

    public boolean isEnum() {
        return "enum".equals(getType());
    }
}
