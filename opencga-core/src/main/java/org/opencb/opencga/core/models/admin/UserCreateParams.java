package org.opencb.opencga.core.models.admin;

import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;

public class UserCreateParams extends org.opencb.opencga.core.models.user.UserCreateParams {

    private Account.AccountType type;

    public UserCreateParams() {
    }

    public UserCreateParams(String id, String name, String email, String password, String organization, Account.AccountType type) {
        super(id, name, email, password, organization);
        this.type = type;
    }

    public static UserCreateParams of(User user) {
        return new UserCreateParams(user.getId(), user.getName(), user.getEmail(), "", user.getOrganization(),
                user.getAccount().getType());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserCreateParams{");
        sb.append("type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public Account.AccountType getType() {
        return type;
    }

    public UserCreateParams setType(Account.AccountType type) {
        this.type = type;
        return this;
    }
}
