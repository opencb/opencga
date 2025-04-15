package org.opencb.opencga.core.models.externalTool;

public class WorkflowRepository {

    private String id;
    private String version;
    private String author;
    private String description;

    public WorkflowRepository() {
    }

    public WorkflowRepository(String id) {
        this.id = id;
    }

    public WorkflowRepository(String id, String version, String author, String description) {
        this.id = id;
        this.version = version;
        this.author = author;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowRepository{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", author='").append(author).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public WorkflowRepository setId(String id) {
        this.id = id;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public WorkflowRepository setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getAuthor() {
        return author;
    }

    public WorkflowRepository setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public WorkflowRepository setDescription(String description) {
        this.description = description;
        return this;
    }
}
