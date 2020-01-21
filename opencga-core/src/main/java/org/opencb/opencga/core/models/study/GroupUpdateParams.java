package org.opencb.opencga.core.models.study;

import java.util.List;

public class GroupUpdateParams {

    private List<String> users;

    public GroupUpdateParams() {
    }

    public GroupUpdateParams(List<String> users) {
        this.users = users;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupUpdateParams{");
        sb.append("users='").append(users).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<String> getUsers() {
        return users;
    }

    public GroupUpdateParams setUsers(List<String> users) {
        this.users = users;
        return this;
    }
}
