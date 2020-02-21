package org.opencb.opencga.core.models.study;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GroupCreateParams {

    @JsonProperty(required = true)
    private String id;
    private List<String> users;

    public GroupCreateParams() {
    }

    public GroupCreateParams(String id, List<String> users) {
        this.id = id;
        this.users = users;
    }

    public static GroupCreateParams of(Group group) {
        return new GroupCreateParams(group.getId(), group.getUserIds());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupCreateParams{");
        sb.append("id='").append(id).append('\'');
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

    public List<String> getUsers() {
        return users;
    }

    public GroupCreateParams setUsers(List<String> users) {
        this.users = users;
        return this;
    }
}
