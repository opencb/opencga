package org.opencb.opencga.core.models;

import org.opencb.opencga.core.models.common.Enums;

public class ToolInfo {

    private String id;
    private String description;

    private Scope scope;
    private Type type;
    private Enums.Resource resource;

    public enum Scope {
        STUDY,
        PROJECT,
        GLOBAL
    }

    public enum Type {
        OPERATION,
        ANALYSIS
    }

    public ToolInfo() {
    }

    public ToolInfo(String id, String description, Scope scope, Type type, Enums.Resource resource) {
        this.id = id;
        this.description = description;
        this.scope = scope;
        this.type = type;
        this.resource = resource;
    }

    public String getId() {
        return id;
    }

    public ToolInfo setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ToolInfo setDescription(String description) {
        this.description = description;
        return this;
    }

    public Scope getScope() {
        return scope;
    }

    public ToolInfo setScope(Scope scope) {
        this.scope = scope;
        return this;
    }

    public Type getType() {
        return type;
    }

    public ToolInfo setType(Type type) {
        this.type = type;
        return this;
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public ToolInfo setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
    }
}
