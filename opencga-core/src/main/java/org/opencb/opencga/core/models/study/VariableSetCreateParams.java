package org.opencb.opencga.core.models.study;

import java.util.List;

public class VariableSetCreateParams {

    private Boolean unique;
    private Boolean confidential;
    private String id;
    private String name;
    private String description;
    private List<String> entities;
    private List<Variable> variables;

    public VariableSetCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariableSetCreateParams{");
        sb.append("unique=").append(unique);
        sb.append(", confidential=").append(confidential);
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
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

    public List<String> getEntities() {
        return entities;
    }

    public VariableSetCreateParams setEntities(List<String> entities) {
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
