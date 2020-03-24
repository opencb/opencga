package org.opencb.opencga.core.models.project;

import java.util.Map;

public class ProjectUpdateParams {
    private String name;
    private String description;
    private ProjectOrganism organism;
    private Map<String, Object> attributes;

    public ProjectUpdateParams() {
    }

    public ProjectUpdateParams(String name, String description, ProjectOrganism organism, Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.organism = organism;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", organism=").append(organism);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public ProjectUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ProjectUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public ProjectUpdateParams setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ProjectUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
