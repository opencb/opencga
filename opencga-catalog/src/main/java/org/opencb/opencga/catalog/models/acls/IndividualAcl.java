package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;

/**
 * Created by pfurio on 11/05/16.
 */
public class IndividualAcl {

    private List<String> users;
    private EnumSet<IndividualPermissions> permissions;

    public enum IndividualPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE,
        CREATE_ANNOTATIONS,
        VIEW_ANNOTATIONS,
        UPDATE_ANNOTATIONS,
        DELETE_ANNOTATIONS
    }

    public IndividualAcl() {
    }

    public IndividualAcl(List<String> users, EnumSet<IndividualPermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public IndividualAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<IndividualPermissions> aux = EnumSet.allOf(IndividualPermissions.class);
        for (IndividualPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public List<String> getUsers() {
        return users;
    }

    public IndividualAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<IndividualPermissions> getPermissions() {
        return permissions;
    }

    public IndividualAcl setPermissions(EnumSet<IndividualPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }
}
