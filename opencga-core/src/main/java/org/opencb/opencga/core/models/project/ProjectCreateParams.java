package org.opencb.opencga.core.models.project;

public class ProjectCreateParams {

    private String id;

    private String name;
    private String description;;
    private ProjectOrganism organism;

    public ProjectCreateParams() {
    }

    public ProjectCreateParams(String id, String name, String description, ProjectOrganism organism) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.organism = organism;
    }

    public static ProjectCreateParams of(Project project) {
        return new ProjectCreateParams(project.getId(), project.getName(), project.getDescription(), project.getOrganism());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
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

    public ProjectOrganism getOrganism() {
        return organism;
    }

    public ProjectCreateParams setOrganism(ProjectOrganism organism) {
        this.organism = organism;
        return this;
    }
}
