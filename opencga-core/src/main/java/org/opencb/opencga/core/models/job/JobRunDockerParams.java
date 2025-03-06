package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.tools.ToolParams;

public class JobRunDockerParams extends ToolParams {

    private String id;
    private String tag;
    private String token;

    public JobRunDockerParams() {
    }

    public JobRunDockerParams(String id, String tag, String token) {
        this.id = id;
        this.tag = tag;
        this.token = token;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobRunDockerParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public JobRunDockerParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public JobRunDockerParams setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getToken() {
        return token;
    }

    public JobRunDockerParams setToken(String token) {
        this.token = token;
        return this;
    }
}
