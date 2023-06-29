package org.opencb.opencga.core.models.admin;

import java.util.Map;

public class TokenParams {

    private String userId;
    private Long expiration;
    private Map<String, Object> attributes;

    public TokenParams() {
    }

    public TokenParams(String userId, Long expiration, Map<String, Object> attributes) {
        this.userId = userId;
        this.expiration = expiration;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TokenParams{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", expiration=").append(expiration);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public TokenParams setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public Long getExpiration() {
        return expiration;
    }

    public TokenParams setExpiration(Long expiration) {
        this.expiration = expiration;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public TokenParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
