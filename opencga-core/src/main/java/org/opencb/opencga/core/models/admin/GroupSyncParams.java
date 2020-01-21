package org.opencb.opencga.core.models.admin;

public class GroupSyncParams {

    private String authenticationOriginId;
    private String from;
    private String to;
    private String study;
    private boolean force;

    public GroupSyncParams() {
    }

    public GroupSyncParams(String authenticationOriginId, String from, String to, String study, boolean force) {
        this.authenticationOriginId = authenticationOriginId;
        this.from = from;
        this.to = to;
        this.study = study;
        this.force = force;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupSyncParams{");
        sb.append("authenticationOriginId='").append(authenticationOriginId).append('\'');
        sb.append(", from='").append(from).append('\'');
        sb.append(", to='").append(to).append('\'');
        sb.append(", study='").append(study).append('\'');
        sb.append(", force=").append(force);
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

    public String getFrom() {
        return from;
    }

    public GroupSyncParams setFrom(String from) {
        this.from = from;
        return this;
    }

    public String getTo() {
        return to;
    }

    public GroupSyncParams setTo(String to) {
        this.to = to;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public GroupSyncParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public GroupSyncParams setForce(boolean force) {
        this.force = force;
        return this;
    }
}
