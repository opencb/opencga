package org.opencb.opencga.core.models.federation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FederationServer {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.FEDERATION_SERVER_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.FEDERATION_SERVER_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "email", description = FieldConstants.FEDERATION_SERVER_EMAIL_DESCRIPTION)
    private String email;

    @DataField(id = "userId", description = FieldConstants.FEDERATION_SERVER_USER_ID_DESCRIPTION)
    private String userId;

    @DataField(id = "active", description = FieldConstants.FEDERATION_SERVER_ACTIVE_DESCRIPTION)
    private boolean active;

    @DataField(id = "expirationTime", description = FieldConstants.FEDERATION_SERVER_EXPIRATION_TIME_DESCRIPTION)
    private String expirationTime;

    @DataField(id = "secretKey", description = FieldConstants.FEDERATION_SERVER_SECRET_KEY_DESCRIPTION)
    private String secretKey;

    public FederationServer() {
    }

    public FederationServer(String id, String description, String email, String userId, boolean active, String expirationTime,
                            String secretKey) {
        this.id = id;
        this.description = description;
        this.email = email;
        this.userId = userId;
        this.active = active;
        this.expirationTime = expirationTime;
        this.secretKey = secretKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationServer{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", active=").append(active);
        sb.append(", expirationTime='").append(expirationTime).append('\'');
        sb.append(", secretKey='").append("xxxxxxxx").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FederationServer setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FederationServer setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationServer setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public FederationServer setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public FederationServer setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getExpirationTime() {
        return expirationTime;
    }

    public FederationServer setExpirationTime(String expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public FederationServer setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }
}
