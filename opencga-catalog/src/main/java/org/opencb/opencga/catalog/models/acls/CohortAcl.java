package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;

/**
 * Created by pfurio on 11/05/16.
 */
public class CohortAcl {

    private List<String> users;
    private EnumSet<CohortPermissions> permissions;

    public enum CohortPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE,
        CREATE_ANNOTATIONS,
        VIEW_ANNOTATIONS,
        UPDATE_ANNOTATIONS,
        DELETE_ANNOTATIONS
    }

    public CohortAcl() {
    }

    public CohortAcl(List<String> users, EnumSet<CohortPermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public CohortAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<CohortPermissions> aux = EnumSet.allOf(CohortPermissions.class);
        for (CohortPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public List<String> getUsers() {
        return users;
    }

    public CohortAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<CohortPermissions> getPermissions() {
        return permissions;
    }

    public CohortAcl setPermissions(EnumSet<CohortPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }
}
