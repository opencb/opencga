package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class FileAcl {

    private List<String> users;
    private EnumSet<FilePermissions> permissions;

    public enum FilePermissions {
        VIEW_HEADER,  // Includes permission to view the sample ids from a VCF file.
        VIEW_CONTENT,
        VIEW,
        CREATE,       // If a folder contains this permission for a user, the user will be able to create files under that folder.
        DELETE,
        UPDATE,       // Modify metadata fields
        DOWNLOAD,
        SHARE
    }

    public FileAcl() {
    }

    public FileAcl(List<String> users, EnumSet<FilePermissions> permissions) {
        this.users = users;
        this.permissions = permissions;
    }

    public FileAcl(List<String> users, ObjectMap permissions) {
        this.users = users;

        EnumSet<FilePermissions> aux = EnumSet.allOf(FilePermissions.class);
        for (FilePermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public FileAcl(List<String> users, List<String> permissions) {
        this.users = users;
        this.permissions = EnumSet.noneOf(FilePermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(FilePermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public List<String> getUsers() {
        return users;
    }

    public FileAcl setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public EnumSet<FilePermissions> getPermissions() {
        return permissions;
    }

    public FileAcl setPermissions(EnumSet<FilePermissions> permissions) {
        this.permissions = permissions;
        return this;
    }
}
