package org.opencb.opencga.core.models.study;

public class GroupSyncParams {

    private String authenticationOriginId;
    private String remoteGroupId;
    private String localGroupId;

    public GroupSyncParams() {
    }

    public GroupSyncParams(String authenticationOriginId, String remoteGroupId, String localGroupId) {
        this.authenticationOriginId = authenticationOriginId;
        this.remoteGroupId = remoteGroupId;
        this.localGroupId = localGroupId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupSyncParams{");
        sb.append("authenticationOriginId='").append(authenticationOriginId).append('\'');
        sb.append(", remoteGroupId='").append(remoteGroupId).append('\'');
        sb.append(", localGroupId='").append(localGroupId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAuthenticationOriginId() {
        return authenticationOriginId;
    }

    public GroupSyncParams setAuthenticationOriginId(String authenticationOriginId) {
        this.authenticationOriginId = authenticationOriginId;
        return this;
    }

    public String getRemoteGroupId() {
        return remoteGroupId;
    }

    public GroupSyncParams setRemoteGroupId(String remoteGroupId) {
        this.remoteGroupId = remoteGroupId;
        return this;
    }

    public String getLocalGroupId() {
        return localGroupId;
    }

    public GroupSyncParams setLocalGroupId(String localGroupId) {
        this.localGroupId = localGroupId;
        return this;
    }
}
