package org.opencb.opencga.core.models.externalTool;

import org.opencb.opencga.core.models.externalTool.workflow.WorkflowRepository;

public class WorkflowRepositoryParams {

    private String name;
    private String tag;
    private String user;
    private String password;

    public WorkflowRepositoryParams() {
    }

    public WorkflowRepositoryParams(String name) {
        this.name = name;
    }

    public WorkflowRepositoryParams(String name, String tag, String user, String password) {
        this.name = name;
        this.tag = tag;
        this.user = user;
        this.password = password;
    }

    public WorkflowRepository toWorkflowRepository() {
        return new WorkflowRepository(name, tag, "", "", user, password);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowRepositoryParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public WorkflowRepositoryParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public WorkflowRepositoryParams setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getUser() {
        return user;
    }

    public WorkflowRepositoryParams setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public WorkflowRepositoryParams setPassword(String password) {
        this.password = password;
        return this;
    }
}
