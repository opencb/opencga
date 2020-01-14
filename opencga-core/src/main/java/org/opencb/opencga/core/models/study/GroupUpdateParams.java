package org.opencb.opencga.core.models.study;

public class GroupUpdateParams {

    private String users;

    public GroupUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupUpdateParams{");
        sb.append("users='").append(users).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getUsers() {
        return users;
    }

    public GroupUpdateParams setUsers(String users) {
        this.users = users;
        return this;
    }
}
