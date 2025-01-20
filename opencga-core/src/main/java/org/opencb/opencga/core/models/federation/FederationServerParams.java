package org.opencb.opencga.core.models.federation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FederationServerParams {

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

    @DataField(id = "securityKey", description = FieldConstants.FEDERATION_SERVER_SECURITY_KEY_DESCRIPTION)
    private String securityKey;

    public FederationServerParams() {
    }

    public FederationServerParams(String id, String description, String email, String userId, boolean active, String securityKey) {
        this.id = id;
        this.description = description;
        this.email = email;
        this.userId = userId;
        this.active = active;
        this.securityKey = securityKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationServer{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", active=").append(active);
        sb.append(", securityKey='").append("xxxxxxxx").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FederationServerParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FederationServerParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationServerParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public FederationServerParams setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public FederationServerParams setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getSecurityKey() {
        return securityKey;
    }

    public FederationServerParams setSecurityKey(String securityKey) {
        this.securityKey = securityKey;
        return this;
    }
}
