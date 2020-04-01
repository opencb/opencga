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

import org.opencb.opencga.core.models.PrivateFields;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jacobo on 12/12/14.
 */
public class VariableSet extends PrivateFields {

    private String id;
    private String name;
    private boolean unique;
    private boolean confidential;
    private String description;
    private Set<Variable> variables;

    private List<AnnotableDataModels> entities;

    private int release;
    private Map<String, Object> attributes;

    public enum AnnotableDataModels {
        SAMPLE,
        COHORT,
        INDIVIDUAL,
        FAMILY,
        FILE
    }

    public VariableSet() {
    }

    public VariableSet(String id, String name, boolean unique, boolean confidential, String description, Set<Variable> variables,
                       List<AnnotableDataModels> entities, int release, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.unique = unique;
        this.confidential = confidential;
        this.description = description;
        this.entities = entities;
        this.release = release;
        this.attributes = attributes;
        this.variables = variables;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariableSet{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", unique=").append(unique);
        sb.append(", confidential=").append(confidential);
        sb.append(", description='").append(description).append('\'');
        sb.append(", variables=").append(variables);
        sb.append(", entities=").append(entities);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public VariableSet setId(String id) {
        this.id = id;
        return this;
    }

    public VariableSet setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    public String getName() {
        return name;
    }

    public VariableSet setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isUnique() {
        return unique;
    }

    public VariableSet setUnique(boolean unique) {
        this.unique = unique;
        return this;
    }

    public boolean isConfidential() {
        return confidential;
    }

    public VariableSet setConfidential(boolean confidential) {
        this.confidential = confidential;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public VariableSet setDescription(String description) {
        this.description = description;
        return this;
    }

    public Set<Variable> getVariables() {
        return variables;
    }

    public VariableSet setVariables(Set<Variable> variables) {
        this.variables = variables;
        return this;
    }

    public List<AnnotableDataModels> getEntities() {
        return entities;
    }

    public VariableSet setEntities(List<AnnotableDataModels> entities) {
        this.entities = entities;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public VariableSet setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public VariableSet setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

}
