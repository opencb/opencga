package org.opencb.opencga.core.models.externalTool;

public class WorkflowRepository {

    private String name;
    private String tag;
    private String author;
    private String description;
    private String user;
    private String password;

    public WorkflowRepository() {
    }

    public WorkflowRepository(String name) {
        this.name = name;
    }

    public WorkflowRepository(String name, String tag, String author, String description, String user, String password) {
        this.name = name;
        this.tag = tag;
        this.author = author;
        this.description = description;
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowRepository{");
        sb.append("name='").append(name).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", author='").append(author).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", user='").append("xxxxxxxx").append('\'');
        sb.append(", password='").append("xxxxxxxx").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public WorkflowRepository setName(String name) {
        this.name = name;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public WorkflowRepository setTag(String tag) {
        this.tag = tag;
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

    public String getUser() {
        return user;
    }

    public WorkflowRepository setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public WorkflowRepository setPassword(String password) {
        this.password = password;
        return this;
    }
}
