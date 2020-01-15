package org.opencb.opencga.core.models.project;

public class ProjectCreateParams {

    private String id;
    @Deprecated
    private String alias;

    private String name;
    private String description;
    private String organization;
    private Project.Organism organism;

    public ProjectCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", organism=").append(organism);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ProjectCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public ProjectCreateParams setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getName() {
        return name;
    }

    public ProjectCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ProjectCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public ProjectCreateParams setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public Project.Organism getOrganism() {
        return organism;
    }

    public ProjectCreateParams setOrganism(Project.Organism organism) {
        this.organism = organism;
        return this;
    }
}
