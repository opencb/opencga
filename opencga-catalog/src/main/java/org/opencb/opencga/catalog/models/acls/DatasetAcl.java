package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 12/05/16.
 */
public class DatasetAcl {

    private List<String> users;
    private EnumSet<DatasetPermissions> permissions;

    public enum DatasetPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DatasetAcl() {
    }

    public DatasetAcl(List<String> users, EnumSet<DatasetPermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public DatasetAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<DatasetPermissions> aux = EnumSet.allOf(DatasetPermissions.class);
        for (DatasetPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DatasetAcl(List<String> users, List<String> permissions) {
        this.users = users;
        this.permissions.addAll(permissions.stream().map(DatasetPermissions::valueOf).collect(Collectors.toList()));
    }

    public List<String> getUsers() {
        return users;
    }

    public DatasetAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<DatasetPermissions> getPermissions() {
        return permissions;
    }

    public DatasetAcl setPermissions(EnumSet<DatasetPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }

}
