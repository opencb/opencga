package org.opencb.opencga.catalog.beans;

import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class SampleAnnotationDescription {

    private String id;
    private String category;

    /**
     * Type accepted values: text, numeric
     */
    private AnnotationType type;
    private Object defaultValue;
    private boolean required;

    /**
     * Example for numeric range: -3,5
     * Example for categorical values: T,F
     */
    private String acceptedValues;
    private String description;

    private Map<String, Object> attributes;

//    private boolean allowed;

    enum AnnotationType {
        TEXT,
        NUMERIC,
        CATEGORICAL
    }

    public SampleAnnotationDescription(String id, String category, AnnotationType type, Object defaultValue, boolean required, String acceptedValues, String description) {
        this.id = id;
        this.category = category;
        this.type = type;
        this.defaultValue = defaultValue;
        this.required = required;
        this.acceptedValues = acceptedValues;
        this.description = description;
    }

    @Override
    public String toString() {
        return "SampleAnnotation{" +
                "id='" + id + '\'' +
                ", category='" + category + '\'' +
                ", type='" + type + '\'' +
                ", defaultValue=" + defaultValue +
                ", required=" + required +
                ", acceptedValues='" + acceptedValues + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public AnnotationType getType() {
        return type;
    }

    public void setType(AnnotationType type) {
        this.type = type;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getAcceptedValues() {
        return acceptedValues;
    }

    public void setAcceptedValues(String acceptedValues) {
        this.acceptedValues = acceptedValues;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
