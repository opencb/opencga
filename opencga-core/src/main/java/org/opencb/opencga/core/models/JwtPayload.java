package org.opencb.opencga.core.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.JwtUtils;
import org.opencb.opencga.core.config.AuthenticationOrigin;

import java.util.Base64;
import java.util.Date;
import java.util.List;

public class JwtPayload {

    private final String userId;
    private final String organization;
    private final AuthenticationOrigin.AuthenticationType authOrigin;
    private final String issuer;         // Issuer of the JWT token.
    private final Date issuedAt;         // Time when the JWT was issued.
    private final Date expirationTime;   // Expiration time of the JWT.
    private final List<Federation> federations; // Federation information containing the projects and studies the user has access to.
    private final String token;

    public static final String AUTH_ORIGIN = "authOrigin";
    public static final String FEDERATIONS = "federations";

    public JwtPayload(String userId, String organization, AuthenticationOrigin.AuthenticationType authOrigin, String issuer, Date issuedAt,
                      Date expirationTime, List<Federation> federationList, String token) {
        this.token = token;
        this.userId = userId;
        this.organization = organization;
        this.authOrigin = authOrigin;
        this.issuer = issuer;
        this.issuedAt = issuedAt;
        this.expirationTime = expirationTime;
        this.federations = federationList;
    }

    /**
     * Parse payload from token to fill the JwtPayload fields.
     * IMPORTANT: This method doesn't validate that the token hasn't been modified !!
     *
     * @param token JWT token.
     */
    public JwtPayload(String token) {
        if (StringUtils.isEmpty(token) || "null".equalsIgnoreCase(token)) {
            throw new IllegalArgumentException("Missing token");
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

            this.token = token;

            this.userId = claimsMap.getString("sub");
            this.organization = claimsMap.getString("aud");
            this.issuer = claimsMap.getString("iss");

            if (claimsMap.containsKey(AUTH_ORIGIN)) {
                this.authOrigin = AuthenticationOrigin.AuthenticationType.valueOf(claimsMap.getString(AUTH_ORIGIN));
            } else {
                this.authOrigin = null;
            }

            if (claimsMap.containsKey("iat")) {
                long iat = 1000L * claimsMap.getLong("iat");
                this.issuedAt = new Date(iat);
            } else {
                this.issuedAt = null;
            }

            if (claimsMap.containsKey("exp")) {
                long exp = 1000L * claimsMap.getLong("exp");
                this.expirationTime = new Date(exp);
            } else {
                this.expirationTime = null;
            }

            this.federations = JwtUtils.getFederations(claimsMap);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JwtPayload{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", authOrigin=").append(authOrigin);
        sb.append(", issuer='").append(issuer).append('\'');
        sb.append(", issuedAt=").append(issuedAt);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append(", federations=").append(federations);
        sb.append(", token='").append(token).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public String getUserId(String organization) {
        if (StringUtils.isNotEmpty(organization) && StringUtils.isNotEmpty(this.organization) && !organization.equals(this.organization)) {
            // User wants to access data from a different organization. Therefore, we need to return an fqn
            return this.organization + ":" + this.userId;
        }
        return this.userId;
    }

    public String getOrganization() {
        return organization;
    }

    public AuthenticationOrigin.AuthenticationType getAuthOrigin() {
        return authOrigin;
    }

    public String getIssuer() {
        return issuer;
    }

    public Date getIssuedAt() {
        return issuedAt;
    }

    public Date getExpirationTime() {
        return expirationTime;
    }

    public String getToken() {
        return token;
    }

    public List<Federation> getFederations() {
        return federations;
    }

    public static class Federation {
        private String id;
        private List<String> projectIds;
        private List<String> studyIds;

        public Federation() {
        }

        public Federation(String id, List<String> projectIds, List<String> studyIds) {
            this.id = id;
            this.projectIds = projectIds;
            this.studyIds = studyIds;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Federation{");
            sb.append("id='").append(id).append('\'');
            sb.append(", projectIds=").append(projectIds);
            sb.append(", studyIds=").append(studyIds);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Federation setId(String id) {
            this.id = id;
            return this;
        }

        public List<String> getProjectIds() {
            return projectIds;
        }

        public Federation setProjectIds(List<String> projectIds) {
            this.projectIds = projectIds;
            return this;
        }

        public List<String> getStudyIds() {
            return studyIds;
        }

        public Federation setStudyIds(List<String> studyIds) {
            this.studyIds = studyIds;
            return this;
        }
    }

}
