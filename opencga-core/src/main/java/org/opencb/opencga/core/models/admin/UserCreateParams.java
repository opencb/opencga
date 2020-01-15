package org.opencb.opencga.core.models.admin;

import org.opencb.opencga.core.models.user.Account;

public class UserCreateParams extends org.opencb.opencga.core.models.user.UserCreateParams {

    private Account.Type type;

    public UserCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserCreateParams{");
        sb.append("type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public Account.Type getType() {
        return type;
    }

    public UserCreateParams setType(Account.Type type) {
        this.type = type;
        return this;
    }
}
