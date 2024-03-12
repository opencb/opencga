package org.opencb.opencga.core.models;

import java.util.List;

public class Acl {

    private String id;
    private String type;
    private List<Permission> permissions;
    private long timestamp;

    public Acl() {
    }

    public Acl(String id, String type, List<Permission> permissions, long timestamp) {
        this.id = id;
        this.type = type;
        this.permissions = permissions;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Acl{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", permissions=").append(permissions);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Acl setId(String id) {
        this.id = id;
        return this;
    }

    public String getType() {
        return type;
    }

    public Acl setType(String type) {
        this.type = type;
        return this;
    }

    public List<Permission> getPermissions() {
        return permissions;
    }

    public Acl setPermissions(List<Permission> permissions) {
        this.permissions = permissions;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Acl setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public static class Permission {
        private String id;
        private List<String> userIds;

        public Permission() {
        }

        public Permission(String id, List<String> userIds) {
            this.id = id;
            this.userIds = userIds;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Permission{");
            sb.append("id='").append(id).append('\'');
            sb.append(", userIds=").append(userIds);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Permission setId(String id) {
            this.id = id;
            return this;
        }

        public List<String> getUserIds() {
            return userIds;
        }

        public Permission setUserIds(List<String> userIds) {
            this.userIds = userIds;
            return this;
        }
    }
}
