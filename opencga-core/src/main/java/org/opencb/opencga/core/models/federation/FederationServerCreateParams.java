package org.opencb.opencga.core.models.federation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FederationServerCreateParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.FEDERATION_CLIENT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.FEDERATION_CLIENT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "email", description = FieldConstants.FEDERATION_CLIENT_EMAIL_DESCRIPTION)
    private String email;

    @DataField(id = "userId", description = FieldConstants.FEDERATION_CLIENT_USER_ID_DESCRIPTION)
    private String userId;

    public FederationServerCreateParams() {
    }

    public FederationServerCreateParams(String id, String description, String email, String userId) {
        this.id = id;
        this.description = description;
        this.email = email;
        this.userId = userId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationServerCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FederationServerCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FederationServerCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationServerCreateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public FederationServerCreateParams setUserId(String userId) {
        this.userId = userId;
        return this;
    }
}
