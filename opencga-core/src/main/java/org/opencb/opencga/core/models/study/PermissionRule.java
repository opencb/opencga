package org.opencb.opencga.core.models.study;

import org.opencb.commons.datastore.core.Query;

import java.util.List;

public class PermissionRule {

    private String id;
    private Query query;
    private List<String> members;
    private List<String> permissions;


    public PermissionRule() {
    }

    public PermissionRule(String id, Query query, List<String> members, List<String> permissions) {
        this.id = id;
        this.query = query;
        this.members = members;
        this.permissions = permissions;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PermissionRules{");
        sb.append("id='").append(id).append('\'');
        sb.append(", query=").append(query);
        sb.append(", members=").append(members);
        sb.append(", permissions=").append(permissions);
        sb.append('}');
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public PermissionRule setId(String id) {
        this.id = id;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public PermissionRule setQuery(Query query) {
        this.query = query;
        return this;
    }

    public List<String> getMembers() {
        return members;
    }

    public PermissionRule setMembers(List<String> members) {
        this.members = members;
        return this;
    }

    public List<String> getPermissions() {
        return permissions;
    }

    public PermissionRule setPermissions(List<String> permissions) {
        this.permissions = permissions;
        return this;
    }

    public enum DeleteAction {
        REMOVE, // Remove all the permissions assigned by the permission rule even if it had been also assigned manually.
        REVERT, // Remove all the permissions assigned by the permission rule but retain manual permissions as well as other permissions
                // that might have been assigned by other permission rules (leave permissions as if the permission rule had never existed).
        NONE,   // Remove the permission rule but no the permissions that might have been eventually assigned because of it.
    }

}
