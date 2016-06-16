package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 12/05/16.
 */
public class DatasetAcl {

    private String member;
    private EnumSet<DatasetPermissions> permissions;

    public enum DatasetPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DatasetAcl() {
    }

    public DatasetAcl(String member, EnumSet<DatasetPermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public DatasetAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<DatasetPermissions> aux = EnumSet.allOf(DatasetPermissions.class);
        for (DatasetPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DatasetAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(DatasetPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(DatasetPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public DatasetAcl setMember(String member) {
        this.member = member;
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
