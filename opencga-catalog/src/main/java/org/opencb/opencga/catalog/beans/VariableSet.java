package org.opencb.opencga.catalog.beans;

import java.util.List;
import java.util.Set;

/**
 * Created by jacobo on 12/12/14.
 */
public class VariableSet {

    public int id;
    public String alias;
    public String name;

    public Set<Variable> variables;

    public VariableSet() {
    }

    public VariableSet(int id, String alias, String name, Set<Variable> variables) {
        this.id = id;
        this.alias = alias;
        this.name = name;
        this.variables = variables;
    }

    @Override
    public String toString() {
        return "VariableSet{" +
                "id=" + id +
                ", alias='" + alias + '\'' +
                ", name='" + name + '\'' +
                ", variables=" + variables +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Variable> getVariables() {
        return variables;
    }

    public void setVariables(Set<Variable> variables) {
        this.variables = variables;
    }
}
