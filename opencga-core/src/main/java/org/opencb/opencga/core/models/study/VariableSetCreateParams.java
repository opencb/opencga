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

import java.util.ArrayList;
import java.util.List;

public class VariableSetCreateParams {

    private String id;
    private String name;
    private Boolean unique;
    private Boolean confidential;
    private String description;
    private List<VariableSet.AnnotableDataModels> entities;
    private List<Variable> variables;

    public VariableSetCreateParams() {
    }

    public VariableSetCreateParams(String id, String name, Boolean unique, Boolean confidential, String description,
                                   List<VariableSet.AnnotableDataModels> entities, List<Variable> variables) {
        this.id = id;
        this.name = name;
        this.unique = unique;
        this.confidential = confidential;
        this.description = description;
        this.entities = entities;
        this.variables = variables;
    }

    public static VariableSetCreateParams of(VariableSet variableSet) {
        return new VariableSetCreateParams(variableSet.getId(), variableSet.getName(), variableSet.isUnique(), variableSet.isConfidential(),
                variableSet.getDescription(), variableSet.getEntities(), new ArrayList<>(variableSet.getVariables()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariableSetCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", unique=").append(unique);
        sb.append(", confidential=").append(confidential);
        sb.append(", description='").append(description).append('\'');
        sb.append(", entities=").append(entities);
        sb.append(", variables=").append(variables);
        sb.append('}');
        return sb.toString();
    }

    public Boolean getUnique() {
        return unique;
    }

    public VariableSetCreateParams setUnique(Boolean unique) {
        this.unique = unique;
        return this;
    }

    public Boolean getConfidential() {
        return confidential;
    }

    public VariableSetCreateParams setConfidential(Boolean confidential) {
        this.confidential = confidential;
        return this;
    }

    public String getId() {
        return id;
    }

    public VariableSetCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public VariableSetCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public VariableSetCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<VariableSet.AnnotableDataModels> getEntities() {
        return entities;
    }

    public VariableSetCreateParams setEntities(List<VariableSet.AnnotableDataModels> entities) {
        this.entities = entities;
        return this;
    }

    public List<Variable> getVariables() {
        return variables;
    }

    public VariableSetCreateParams setVariables(List<Variable> variables) {
        this.variables = variables;
        return this;
    }
}
