package org.opencb.opencga.core.models.user;

import java.util.Map;

public class UserUpdateParams {

    private String name;
    private String email;
    private String organization;
    private Map<String, Object> attributes;

    public UserUpdateParams() {
    }

    public UserUpdateParams(String name, String email, String organization, Map<String, Object> attributes) {
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public UserUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public UserUpdateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public UserUpdateParams setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public UserUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
