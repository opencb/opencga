package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class IndividualAcl {

    private String member;
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

    public IndividualAcl(String member, EnumSet<IndividualPermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public IndividualAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<IndividualPermissions> aux = EnumSet.allOf(IndividualPermissions.class);
        for (IndividualPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public IndividualAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(IndividualPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(IndividualPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public IndividualAcl setMember(String member) {
        this.member = member;
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
