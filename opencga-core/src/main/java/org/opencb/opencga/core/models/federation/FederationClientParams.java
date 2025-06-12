package org.opencb.opencga.core.models.federation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FederationClientParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.FEDERATION_CLIENT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.FEDERATION_CLIENT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "version", description = FieldConstants.FEDERATION_CLIENT_VERSION_DESCRIPTION)
    private String version;

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

    @DataField(id = "securityKey", description = FieldConstants.FEDERATION_CLIENT_SECURITY_KEY_DESCRIPTION)
    private String securityKey;

    public FederationClientParams() {
    }

    public FederationClientParams(String id, String description, String version, String email, String url, String organizationId,
                                  String userId, String password, String token, String securityKey) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.email = email;
        this.url = url;
        this.organizationId = organizationId;
        this.userId = userId;
        this.password = password;
        this.token = token;
        this.securityKey = securityKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationClient{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", organizationId='").append(organizationId).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", password='").append("xxxxxxxx").append('\'');
        sb.append(", securityKey='").append("xxxxxxxx").append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FederationClientParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FederationClientParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public FederationClientParams setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationClientParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public FederationClientParams setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public FederationClientParams setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public FederationClientParams setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public FederationClientParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getSecurityKey() {
        return securityKey;
    }

    public FederationClientParams setSecurityKey(String securityKey) {
        this.securityKey = securityKey;
        return this;
    }

    public String getToken() {
        return token;
    }

    public FederationClientParams setToken(String token) {
        this.token = token;
        return this;
    }
}
