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
    private String id;

    /**
     * Set of users belonging to this group.
     */
    private List<String> userIds;

    public Group() {
    }

    public Group(String id, List<String> userIds) {
        this.id = id;
        this.userIds = userIds;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Group{");
        sb.append("id='").append(id).append('\'');
        sb.append(", userIds=").append(userIds);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Group setId(String id) {
        this.id = id;
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

