package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class JobAcl {

    private String member;
    private EnumSet<JobPermissions> permissions;

    public enum JobPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public JobAcl() {
    }

    public JobAcl(String member, EnumSet<JobPermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public JobAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<JobPermissions> aux = EnumSet.allOf(JobPermissions.class);
        for (JobPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public JobAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(JobPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(JobPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public JobAcl setMember(String member) {
        this.member = member;
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
