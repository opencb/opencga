package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class SampleAcl {

    private List<String> users;
    private EnumSet<SamplePermissions> permissions;

    public enum SamplePermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE,
        CREATE_ANNOTATIONS,
        VIEW_ANNOTATIONS,
        UPDATE_ANNOTATIONS,
        DELETE_ANNOTATIONS
    }

    public SampleAcl() {
    }

    public SampleAcl(List<String> users, EnumSet<SamplePermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public SampleAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<SamplePermissions> aux = EnumSet.allOf(SamplePermissions.class);
        for (SamplePermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public SampleAcl(List<String> users, List<String> permissions) {
        this.users = users;
        this.permissions = EnumSet.noneOf(SamplePermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(SamplePermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public List<String> getUsers() {
        return users;
    }

    public SampleAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<SamplePermissions> getPermissions() {
        return permissions;
    }

    public SampleAcl setPermissions(EnumSet<SamplePermissions> permissions) {
        this.permissions = permissions;
        return this;
    }
}
