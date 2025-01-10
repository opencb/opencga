package org.opencb.opencga.core.models.federation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FederationClient {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.FEDERATION_CLIENT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.FEDERATION_CLIENT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "email", description = FieldConstants.FEDERATION_CLIENT_EMAIL_DESCRIPTION)
    private String email;

    @DataField(id = "url", description = FieldConstants.FEDERATION_CLIENT_URL_DESCRIPTION)
    private String url;

    @DataField(id = "organizationId", description = FieldConstants.FEDERATION_CLIENT_ORGANIZATION_ID_DESCRIPTION)
    private String organizationId;

    @DataField(id = "userId", description = FieldConstants.FEDERATION_CLIENT_USER_ID_DESCRIPTION)
    private String userId;

    @DataField(id = "password", description = FieldConstants.FEDERATION_CLIENT_PASSWORD_DESCRIPTION)
    private String password;

    @DataField(id = "token", description = FieldConstants.FEDERATION_CLIENT_TOKEN_DESCRIPTION)
    private String token;

    @DataField(id = "secretKey", description = FieldConstants.FEDERATION_CLIENT_SECRET_KEY_DESCRIPTION)
    private String secretKey;

    public FederationClient() {
    }

    public FederationClient(String id, String description, String email, String url, String organizationId, String userId, String password,
                            String secretKey) {
        this.id = id;
        this.description = description;
        this.email = email;
        this.url = url;
        this.organizationId = organizationId;
        this.userId = userId;
        this.password = password;
        this.secretKey = secretKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationClient{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", organizationId='").append(organizationId).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", password='").append("xxxxxxxx").append('\'');
        sb.append(", secretKey='").append("xxxxxxxx").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FederationClient setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FederationClient setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationClient setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public FederationClient setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public FederationClient setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public FederationClient setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public FederationClient setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public FederationClient setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
