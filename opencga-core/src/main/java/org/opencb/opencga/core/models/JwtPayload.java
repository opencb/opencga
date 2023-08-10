package org.opencb.opencga.core.models;

import java.util.Date;

public class JwtPayload {

    private String userId;
    private String organization;
    private String audience;      // Recipients of the JWT token.
    private Date issuedAt;        // Time when the JWT was issued.
    private Date expirationTime;  // Expiration time of the JWT.

    public JwtPayload() {
    }

    public JwtPayload(String userId, String organization, String audience, Date issuedAt, Date expirationTime) {
        this.userId = userId;
        this.organization = organization;
        this.audience = audience;
        this.issuedAt = issuedAt;
        this.expirationTime = expirationTime;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JwtPayload{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", audience='").append(audience).append('\'');
        sb.append(", issuedAt=").append(issuedAt);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public JwtPayload setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public JwtPayload setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public String getAudience() {
        return audience;
    }

    public JwtPayload setAudience(String audience) {
        this.audience = audience;
        return this;
    }

    public Date getIssuedAt() {
        return issuedAt;
    }

    public JwtPayload setIssuedAt(Date issuedAt) {
        this.issuedAt = issuedAt;
        return this;
    }

    public Date getExpirationTime() {
        return expirationTime;
    }

    public JwtPayload setExpirationTime(Date expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }
}
