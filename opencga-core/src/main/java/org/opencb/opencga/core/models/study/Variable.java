/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.models.study;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by jacobo on 11/09/14.
 */
public class Variable {

    private String id;
    private String name;
    private String category;

    /**
     * Type accepted values: text, numeric.
     */
    private VariableType type;
    private Object defaultValue;
    private boolean required;
    private boolean multiValue;

    /**
     * Example for numeric range: -3:5.
     * Example for categorical values: T,F
     */
    private List<String> allowedValues;
    private List<String> allowedKeys;
    private long rank;
    private String dependsOn;
    private String description;
    /**
     * Variables for validate internal fields. Only valid if type is OBJECT.
     **/
    private Set<Variable> variableSet;
    private Map<String, Object> attributes;

    public Variable() {
    }

    public Variable(String id, String name, String category, VariableType type, Object defaultValue, boolean required,
                    boolean multiValue, List<String> allowedValues, List<String> allowedKeys, long rank, String dependsOn,
                    String description, Set<Variable> variableSet, Map<String, Object> attributes) {
        this.name = name;
        this.id = id;
        this.category = category;
        this.type = type;
        this.defaultValue = defaultValue;
        this.required = required;
        this.multiValue = multiValue;
        this.allowedValues = allowedValues;
        this.allowedKeys = allowedKeys;
        this.rank = rank;
        this.dependsOn = dependsOn;
        this.description = description;
        this.variableSet = variableSet;
        this.attributes = attributes;
    }

    @Deprecated
    public Variable(String id, String category, VariableType type, Object defaultValue, boolean required, boolean multiValue,
                    List<String> allowedValues, List<String> allowedKeys, long rank, String dependsOn, String description,
                    Set<Variable> variableSet, Map<String, Object> attributes) {
        this(id, "", category, type, defaultValue, required, multiValue, allowedValues, allowedKeys, rank, dependsOn, description,
                variableSet, attributes);
    }

    public enum VariableType {
        BOOLEAN,
        CATEGORICAL,
        INTEGER,
        DOUBLE,
        @Deprecated
        TEXT,
        STRING,
        OBJECT,
        MAP_BOOLEAN,
        MAP_INTEGER,
        MAP_DOUBLE,
        MAP_STRING
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Variable{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", category='").append(category).append('\'');
        sb.append(", type=").append(type);
        sb.append(", defaultValue=").append(defaultValue);
        sb.append(", required=").append(required);
        sb.append(", multiValue=").append(multiValue);
        sb.append(", allowedValues=").append(allowedValues);
        sb.append(", allowedKeys=").append(allowedKeys);
        sb.append(", rank=").append(rank);
        sb.append(", dependsOn='").append(dependsOn).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", variableSet=").append(variableSet);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Variable setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Variable setName(String name) {
        this.name = name;
        return this;
    }

    public String getCategory() {
        return category;
    }

    public Variable setCategory(String category) {
        this.category = category;
        return this;
    }

    public VariableType getType() {
        return type;
    }

    public Variable setType(VariableType type) {
        this.type = type;
        return this;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Variable setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public boolean isRequired() {
        return required;
    }

    public Variable setRequired(boolean required) {
        this.required = required;
        return this;
    }

    public boolean isMultiValue() {
        return multiValue;
    }

    public Variable setMultiValue(boolean multiValue) {
        this.multiValue = multiValue;
        return this;
    }

    public List<String> getAllowedValues() {
        return allowedValues;
    }

    public Variable setAllowedValues(List<String> allowedValues) {
        this.allowedValues = allowedValues;
        return this;
    }

    public List<String> getAllowedKeys() {
        return allowedKeys;
    }

    public Variable setAllowedKeys(List<String> allowedKeys) {
        this.allowedKeys = allowedKeys;
        return this;
    }

    public long getRank() {
        return rank;
    }

    public Variable setRank(long rank) {
        this.rank = rank;
        return this;
    }

    public String getDependsOn() {
        return dependsOn;
    }

    public Variable setDependsOn(String dependsOn) {
        this.dependsOn = dependsOn;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Variable setDescription(String description) {
        this.description = description;
        return this;
    }

    public Set<Variable> getVariableSet() {
        return variableSet;
    }

    public Variable setVariableSet(Set<Variable> variableSet) {
        this.variableSet = variableSet;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Variable setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Variable variable = (Variable) o;
        return required == variable.required
                && multiValue == variable.multiValue
                && rank == variable.rank
                && Objects.equals(id, variable.id)
                && Objects.equals(name, variable.name)
                && Objects.equals(category, variable.category)
                && type == variable.type
                && Objects.equals(defaultValue, variable.defaultValue)
                && Objects.equals(allowedValues, variable.allowedValues)
                && Objects.equals(dependsOn, variable.dependsOn)
                && Objects.equals(description, variable.description)
                && Objects.equals(variableSet, variable.variableSet)
                && Objects.equals(attributes, variable.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, category, type, defaultValue, required, multiValue, allowedValues, rank, dependsOn,
                description, variableSet, attributes);
    }
}
