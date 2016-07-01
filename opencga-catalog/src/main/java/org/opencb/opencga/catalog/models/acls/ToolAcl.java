package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sgallego on 6/30/16.
 */
public class ToolAcl{

    private String member;
    private EnumSet<ToolPermissions> permissions;

    public enum ToolPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public ToolAcl() {
    }

    public ToolAcl(String member, EnumSet<ToolPermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public ToolAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<ToolPermissions> aux = EnumSet.allOf(ToolPermissions.class);
        for (ToolPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public ToolAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(ToolPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(ToolPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public ToolAcl setMember(String member) {
        this.member = member;
        return this;
    }

    public EnumSet<ToolPermissions> getPermissions() {
        return permissions;
    }

    public ToolAcl setPermissions(EnumSet<ToolPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }
}

