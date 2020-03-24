package org.opencb.opencga.core.models.user;

public class UserInternal {

    private UserStatus status;

    public UserInternal() {
    }

    public UserInternal(UserStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public UserStatus getStatus() {
        return status;
    }

    public UserInternal setStatus(UserStatus status) {
        this.status = status;
        return this;
    }
}
