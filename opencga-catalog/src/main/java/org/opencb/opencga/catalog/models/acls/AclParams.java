package org.opencb.opencga.catalog.models.acls;

/**
 * Created by pfurio on 29/03/17.
 */
public class AclParams {

    protected String permissions;
    protected Action action;

    public enum Action {
        SET,
        ADD,
        REMOVE,
        RESET
    }
    public AclParams() {
    }

    public AclParams(String permissions, Action action) {
        this.permissions = permissions;
        this.action = action;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }


    public String getPermissions() {
        return permissions;
    }

    public AclParams setPermissions(String permissions) {
        this.permissions = permissions;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public AclParams setAction(Action action) {
        this.action = action;
        return this;
    }
}
