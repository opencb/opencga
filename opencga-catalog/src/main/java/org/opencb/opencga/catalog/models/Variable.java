/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.models;

import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Variable {

    private String id;
    private String category;

    /**
     * Type accepted values: text, numeric
     */
    private VariableType type;
    private Object defaultValue;
    private boolean required;

    /**
     * Example for numeric range: -3:5
     * Example for categorical values: T,F
     */
    private List<String> allowedValues;
    private int rank;
    private String dependsOn;
    private String description;

    private Map<String, Object> attributes;

    public enum VariableType {
        BOOLEAN,
        CATEGORICAL,
        NUMERIC,
        TEXT
    }

    public Variable() {
    }

    public Variable(String id, String category, VariableType type, Object defaultValue, boolean required,
                    List<String> allowedValues, int rank, String dependsOn, String description,
                    Map<String, Object> attributes) {
        this.id = id;
        this.category = category;
        this.type = type;
        this.defaultValue = defaultValue;
        this.required = required;
        this.allowedValues = allowedValues;
        this.rank = rank;
        this.dependsOn = dependsOn;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Variable{" +
                "id='" + id + '\'' +
                ", category='" + category + '\'' +
                ", type=" + type +
                ", defaultValue=" + defaultValue +
                ", required=" + required +
                ", allowedValues=" + allowedValues +
                ", rank=" + rank +
                ", dependsOn='" + dependsOn + '\'' +
                ", description='" + description + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Variable)) return false;

        Variable variable = (Variable) o;

        if (!id.equals(variable.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
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

    public VariableType getType() {
        return type;
    }

    public void setType(VariableType type) {
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

    public List<String> getAllowedValues() {
        return allowedValues;
    }

    public void setAllowedValues(List<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public String getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(String dependsOn) {
        this.dependsOn = dependsOn;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
