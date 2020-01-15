package org.opencb.opencga.core.models.user;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PasswordChangeParams {

    @JsonProperty(required = true)
    private String password;
    @Deprecated
    private String npassword;
    @JsonProperty(required = true)
    private String newPassword;

    public PasswordChangeParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PasswordChangeParams{");
        sb.append("password='").append(password).append('\'');
        sb.append(", npassword='").append(npassword).append('\'');
        sb.append(", newPassword='").append(newPassword).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPassword() {
        return password;
    }

    public PasswordChangeParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getNpassword() {
        return npassword;
    }

    public PasswordChangeParams setNpassword(String npassword) {
        this.npassword = npassword;
        return this;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public PasswordChangeParams setNewPassword(String newPassword) {
        this.newPassword = newPassword;
        return this;
    }
}
