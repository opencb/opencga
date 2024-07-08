package org.opencb.opencga.core.models.user;

public class UserStatusUpdateParams {

    private String status;

    public UserStatusUpdateParams() {
    }

    public UserStatusUpdateParams(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserStatusUpdateParams{");
        sb.append("status='").append(status).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStatus() {
        return status;
    }

    public UserStatusUpdateParams setStatus(String status) {
        this.status = status;
        return this;
    }
}
