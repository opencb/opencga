package org.opencb.opencga.core.models.user;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

public class UserCreateParams {

    @JsonProperty(required = true)
    private String id;
    @JsonProperty(required = true)
    private String name;
    @JsonProperty(required = true)
    private String email;
    @JsonProperty(required = true)
    private String password;
    private String organization;

    public UserCreateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public boolean checkValidParams() {
        if (StringUtils.isEmpty("id") || StringUtils.isEmpty("name") || StringUtils.isEmpty("email")
                || StringUtils.isEmpty("password")) {
            return false;
        }
        return true;
    }

    public String getId() {
        return id;
    }

    public UserCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public UserCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public UserCreateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public UserCreateParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public UserCreateParams setOrganization(String organization) {
        this.organization = organization;
        return this;
    }
}
