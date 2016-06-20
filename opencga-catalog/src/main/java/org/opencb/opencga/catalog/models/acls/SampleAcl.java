package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class SampleAcl {

    private String member;
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

    public SampleAcl(String member, EnumSet<SamplePermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public SampleAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<SamplePermissions> aux = EnumSet.allOf(SamplePermissions.class);
        for (SamplePermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public SampleAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(SamplePermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(SamplePermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public SampleAcl setMember(String member) {
        this.member = member;
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
