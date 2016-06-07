package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 01/06/16.
 */
public class DiseasePanelAcl {

    private List<String> users;
    private EnumSet<DiseasePanelPermissions> permissions;

    public enum DiseasePanelPermissions {
        VIEW,
        UPDATE,
        DELETE,
        SHARE
    }

    public DiseasePanelAcl() {
    }

    public DiseasePanelAcl(List<String> users, EnumSet<DiseasePanelPermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public DiseasePanelAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<DiseasePanelPermissions> aux = EnumSet.allOf(DiseasePanelPermissions.class);
        for (DiseasePanelPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public DiseasePanelAcl(List<String> users, List<String> permissions) {
        this.users = users;
        this.permissions = EnumSet.noneOf(DiseasePanelPermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(DiseasePanelPermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public List<String> getUsers() {
        return users;
    }

    public DiseasePanelAcl setUsers(List<String> users) {
        this.users = users;
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
