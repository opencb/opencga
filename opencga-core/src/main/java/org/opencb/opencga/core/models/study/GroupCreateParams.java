package org.opencb.opencga.core.models.study;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GroupCreateParams {

    @JsonProperty(required = true)
    private String id;
    private String name;
    private List<String> users;

    public GroupCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", users='").append(users).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public GroupCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public GroupCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public List<String> getUsers() {
        return users;
    }

    public GroupCreateParams setUsers(List<String> users) {
        this.users = users;
        return this;
    }
}
