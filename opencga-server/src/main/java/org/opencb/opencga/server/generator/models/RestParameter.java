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
        return (!isComplex() || isStringList());
    }

    public boolean isCollection() {
        return isComplex() && ("List".equals(getType()) || "Map".equals(getType()));
    }

    @Override
    public String toString() {
        return "RestParameter{" +
                "name='" + name + '\'' +
                ", param='" + param + '\'' +
                ", parentParamName='" + parentName + '\'' +
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

    public void setName(String name) {
        this.name = name;
    }

    public RestParamType getParam() {
        return param;
    }

    public void setParam(RestParamType param) {
        this.param = param;
    }

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTypeClass() {
        return typeClass;
    }

    public void setTypeClass(String typeClass) {
        this.typeClass = typeClass;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<RestParameter> getData() {
        return data;
    }

    public void setData(List<RestParameter> data) {
        this.data = data;
    }

    public String getAllowedValues() {
        return allowedValues;
    }

    public void setAllowedValues(String allowedValues) {
        this.allowedValues = allowedValues;
    }

    public boolean isComplex() {
        return complex;
    }

    public void setComplex(boolean complex) {
        this.complex = complex;
    }

    public String getGenericType() {
        return genericType;
    }

    public void setGenericType(String genericType) {
        this.genericType = genericType;
    }

    public boolean isInnerParam() {
        return innerParam;
    }

    public void setInnerParam(boolean innerParam) {
        this.innerParam = innerParam;
    }
}
