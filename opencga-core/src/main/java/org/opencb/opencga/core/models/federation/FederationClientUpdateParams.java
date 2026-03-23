package org.opencb.opencga.core.models.federation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class FederationClientUpdateParams {

    @DataField(id = "description", description = FieldConstants.FEDERATION_CLIENT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "version", description = FieldConstants.FEDERATION_CLIENT_VERSION_DESCRIPTION)
    private String version;

    @DataField(id = "email", description = FieldConstants.FEDERATION_CLIENT_EMAIL_DESCRIPTION)
    private String email;

    @DataField(id = "url", description = FieldConstants.FEDERATION_CLIENT_URL_DESCRIPTION)
    private String url;

    @DataField(id = "password", description = FieldConstants.FEDERATION_CLIENT_PASSWORD_DESCRIPTION)
    private String password;

    @DataField(id = "token", description = FieldConstants.FEDERATION_CLIENT_TOKEN_DESCRIPTION)
    private String token;

    @DataField(id = "securityKey", description = FieldConstants.FEDERATION_CLIENT_SECURITY_KEY_DESCRIPTION)
    private String securityKey;

    public FederationClientUpdateParams() {
    }

    public FederationClientUpdateParams(String description, String version, String email, String url, String password, String token,
                                        String securityKey) {
        this.description = description;
        this.version = version;
        this.email = email;
        this.url = url;
        this.password = password;
        this.token = token;
        this.securityKey = securityKey;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationClientUpdateParams{");
        sb.append("description='").append(description).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append(", securityKey='").append(securityKey).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public FederationClientUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public FederationClientUpdateParams setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationClientUpdateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public FederationClientUpdateParams setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public FederationClientUpdateParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getToken() {
        return token;
    }

    public FederationClientUpdateParams setToken(String token) {
        this.token = token;
        return this;
    }

    public String getSecurityKey() {
        return securityKey;
    }

    public FederationClientUpdateParams setSecurityKey(String securityKey) {
        this.securityKey = securityKey;
        return this;
    }
}
