package org.opencb.opencga.catalog.models;

import java.util.List;

/**
 * Created on 21/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class Group {

    /**
     * Group name, unique in the belonging study
     */
    private String name;

    /**
     * Set of users belonging to this group
     */
    private List<String> userIds;

    /**
     * Group permissions over one study
     */
    private GroupPermissions permissions;

    public Group() {
    }

    public Group(String name, List<String> userIds, GroupPermissions permissions) {
        this.name = name;
        this.userIds = userIds;
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        return "Group{" +
                "name='" + name + '\'' +
                ", userIds='" + userIds + '\'' +
                ", permissions=" + permissions +
                '}';
    }

    public String getName() {
        return name;
    }

    public Group setName(String name) {
        this.name = name;
        return this;
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public Group setUserIds(List<String> userIds) {
        this.userIds = userIds;
        return this;
    }

    public GroupPermissions getPermissions() {
        return permissions;
    }

    public Group setPermissions(GroupPermissions permissions) {
        this.permissions = permissions;
        return this;
    }
}

