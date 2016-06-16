package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 01/06/16.
 */
public class DiseasePanelAcl {

    private String member;
    private EnumSet<DiseasePanelPermissions> permissions;

    public enum DiseasePanelPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DiseasePanelAcl() {
    }

    public DiseasePanelAcl(String member, EnumSet<DiseasePanelPermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public DiseasePanelAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<DiseasePanelPermissions> aux = EnumSet.allOf(DiseasePanelPermissions.class);
        for (DiseasePanelPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DiseasePanelAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(DiseasePanelPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(DiseasePanelPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public DiseasePanelAcl setMember(String member) {
        this.member = member;
        return this;
    }

    public EnumSet<DiseasePanelPermissions> getPermissions() {
        return permissions;
    }

    public DiseasePanelAcl setPermissions(EnumSet<DiseasePanelPermissions> permissions) {
        this.permissions = permissions;
        return this;
    }

}
