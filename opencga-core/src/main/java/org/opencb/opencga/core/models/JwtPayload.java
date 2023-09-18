package org.opencb.opencga.core.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;

import java.util.Base64;
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

    /**
     * Parse payload from token to fill the JwtPayload fields.
     * IMPORTANT: This method doesn't validate that the token hasn't been modified !!
     *
     * @param token JWT token.
     */
    public JwtPayload(String token) {
        if (StringUtils.isEmpty(token) || "null".equalsIgnoreCase(token)) {
            this.userId = ParamConstants.ANONYMOUS_USER_ID;
        } else {
            // Analyse token
            String[] split = StringUtils.split(token, ".");
            if (split.length != 3) {
                throw new IllegalArgumentException("Invalid JWT token");
            }
            String claims = new String(Base64.getDecoder().decode(split[1]));

            ObjectMap claimsMap;
            try {
                claimsMap = JacksonUtils.getDefaultObjectMapper().readerFor(ObjectMap.class).readValue(claims);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid JWT token. Token could not be parsed.", e);
            }

            this.userId = claimsMap.getString("sub");
            this.audience = claimsMap.getString("aud");
            this.organization = claimsMap.getString("bla");

            if (claimsMap.containsKey("iat")) {
                long iat = 1000L * claimsMap.getLong("iat");
                this.issuedAt = new Date(iat);
            }

            if (claimsMap.containsKey("exp")) {
                long exp = 1000L * claimsMap.getLong("exp");
                this.expirationTime = new Date(exp);
            }
        }
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

    public String getOrganization() {
        return organization;
    }

    public String getAudience() {
        return audience;
    }

    public Date getIssuedAt() {
        return issuedAt;
    }

    public Date getExpirationTime() {
        return expirationTime;
    }
}
