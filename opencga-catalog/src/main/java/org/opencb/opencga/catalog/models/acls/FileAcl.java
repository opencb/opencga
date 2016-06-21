package org.opencb.opencga.catalog.models.acls;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class FileAcl {

    private String member;
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

    public FileAcl(String member, EnumSet<FilePermissions> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public FileAcl(String member, ObjectMap permissions) {
        this.member = member;

        EnumSet<FilePermissions> aux = EnumSet.allOf(FilePermissions.class);
        for (FilePermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public FileAcl(String member, List<String> permissions) {
        this.member = member;
        this.permissions = EnumSet.noneOf(FilePermissions.class);
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(FilePermissions::valueOf).collect(Collectors.toList()));
        }
    }

    public String getMember() {
        return member;
    }

    public FileAcl setMember(String member) {
        this.member = member;
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
