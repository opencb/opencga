package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.user.User;

import java.util.List;

public class CustomGroup {

    /**
     * Group id, unique in the belonging study.
     */
    private String id;

    /**
     * Set of users belonging to this group.
     */
    private List<User> users;

    /**
     * Group has been synchronised from an external authorization.
     */
    private Group.Sync syncedFrom;

    public CustomGroup() {
    }

    public CustomGroup(String id, List<User> users, Group.Sync syncedFrom) {
        this.id = id;
        this.users = users;
        this.syncedFrom = syncedFrom;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomGroup{");
        sb.append("id='").append(id).append('\'');
        sb.append(", users=").append(users);
        sb.append(", syncedFrom=").append(syncedFrom);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CustomGroup setId(String id) {
        this.id = id;
        return this;
    }

    public List<User> getUsers() {
        return users;
    }

    public CustomGroup setUsers(List<User> users) {
        this.users = users;
        return this;
    }

    public Group.Sync getSyncedFrom() {
        return syncedFrom;
    }

    public CustomGroup setSyncedFrom(Group.Sync syncedFrom) {
        this.syncedFrom = syncedFrom;
        return this;
    }
}
