package org.opencb.opencga.catalog.beans;

import java.util.Map;
import java.util.Set;

/**
 * Created by jacobo on 12/12/14.
 */
public class VariableSet {

    private int id;
    private String name;
    private boolean repeatable;

    private String description;
    private Set<Variable> variables;

    private Map<String, Object> attributes;

    public VariableSet() {
    }

    public VariableSet(int id, String name, boolean repeatable, String description, Set<Variable> variables, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.repeatable = repeatable;
        this.description = description;
        this.attributes = attributes;
        this.variables = variables;
    }

    @Override
    public String toString() {
        return "VariableSet{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", repeatable=" + repeatable +
                ", description='" + description + '\'' +
                ", variables=" + variables +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isRepeatable() {
        return repeatable;
    }

    public void setRepeatable(boolean repeatable) {
        this.repeatable = repeatable;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Set<Variable> getVariables() {
        return variables;
    }

    public void setVariables(Set<Variable> variables) {
        this.variables = variables;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
