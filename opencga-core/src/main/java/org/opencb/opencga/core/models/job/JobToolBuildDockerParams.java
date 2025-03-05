package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.tools.ToolParams;

public class JobToolBuildDockerParams extends ToolParams {

    private String organisation;
    private String name;
    private String tag;
    private String user;
    private String password;

    public JobToolBuildDockerParams() {
    }

    public JobToolBuildDockerParams(String organisation, String name, String tag, String user, String password) {
        this.organisation = organisation;
        this.name = name;
        this.tag = tag;
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobToolBuildDockerParams{");
        sb.append("organisation='").append(organisation).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOrganisation() {
        return organisation;
    }

    public JobToolBuildDockerParams setOrganisation(String organisation) {
        this.organisation = organisation;
        return this;
    }

    public String getName() {
        return name;
    }

    public JobToolBuildDockerParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public JobToolBuildDockerParams setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getUser() {
        return user;
    }

    public JobToolBuildDockerParams setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public JobToolBuildDockerParams setPassword(String password) {
        this.password = password;
        return this;
    }
}
