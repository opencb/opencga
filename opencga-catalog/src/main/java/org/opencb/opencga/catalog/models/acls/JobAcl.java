package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class JobAcl {

    private List<String> users;
    private EnumSet<JobPermissions> permissions;

    public enum JobPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public JobAcl() {
    }

    public JobAcl(List<String> users, EnumSet<JobPermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public JobAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<JobPermissions> aux = EnumSet.allOf(JobPermissions.class);
        for (JobPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public JobAcl(List<String> users, List<String> permissions) {
        this.users = users;
        this.permissions = EnumSet.noneOf(JobPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(JobPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public List<String> getUsers() {
        return users;
    }

    public JobAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<JobPermissions> getPermissions() {
        return permissions;
    }

    public JobAcl setPermissions(EnumSet<JobPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }
}
