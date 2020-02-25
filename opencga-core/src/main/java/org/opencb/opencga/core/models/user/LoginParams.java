package org.opencb.opencga.core.models.user;

public class LoginParams {

    private String password;

    public LoginParams() {
    }

    public LoginParams(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LoginParams{");
        sb.append("password='").append("********").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPassword() {
        return password;
    }

    public LoginParams setPassword(String password) {
        this.password = password;
        return this;
    }
}
