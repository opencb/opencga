package org.opencb.opencga.catalog.models;

import java.util.List;

/**
 * Created on 21/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class Group {

    /**
     * Group name, unique in the belonging study.
     */
    private String name;

    /**
     * Set of users belonging to this group.
     */
    private List<String> userIds;

    public Group() {
    }

    public Group(String name, List<String> userIds) {
        this.name = name;
        this.userIds = userIds;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Group{");
        sb.append("name='").append(name).append('\'');
        sb.append(", userIds=").append(userIds);
        sb.append('}');
        return sb.toString();
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

}

