package org.opencb.opencga.core.config;

public class UriCheck {

    private String path;
    private Permission permission;

    public enum Permission {
        READ,
        WRITE
    }

    public UriCheck() {
    }

    public UriCheck(String path, Permission permission) {
        this.path = path;
        this.permission = permission;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UriCheck{");
        sb.append("path='").append(path).append('\'');
        sb.append(", permission=").append(permission);
        sb.append('}');
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public UriCheck setPath(String path) {
        this.path = path;
        return this;
    }

    public Permission getPermission() {
        return permission;
    }

    public UriCheck setPermission(Permission permission) {
        this.permission = permission;
        return this;
    }
}
