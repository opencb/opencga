package com.zettagenomics.opencga.enterprise.core.models.federation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class FederationServerUpdateParams {

    @DataField(id = "description", description = FieldConstants.FEDERATION_SERVER_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "email", description = FieldConstants.FEDERATION_SERVER_EMAIL_DESCRIPTION)
    private String email;

    @DataField(id = "active", description = FieldConstants.FEDERATION_SERVER_ACTIVE_DESCRIPTION)
    private Boolean active;

    @DataField(id = "expirationTime", description = FieldConstants.FEDERATION_SERVER_EXPIRATION_TIME_DESCRIPTION)
    private String expirationTime;

    @DataField(id = "securityKey", description = FieldConstants.FEDERATION_SERVER_SECURITY_KEY_DESCRIPTION)
    private String securityKey;

    public FederationServerUpdateParams() {
    }

    public FederationServerUpdateParams(String description, String email, Boolean active, String expirationTime, String securityKey) {
        this.description = description;
        this.email = email;
        this.active = active;
        this.expirationTime = expirationTime;
        this.securityKey = securityKey;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationServerUpdateParams{");
        sb.append("description='").append(description).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", active=").append(active);
        sb.append(", expirationTime='").append(expirationTime).append('\'');
        sb.append(", securityKey='").append(securityKey).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public FederationServerUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public FederationServerUpdateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public Boolean getActive() {
        return active;
    }

    public FederationServerUpdateParams setActive(Boolean active) {
        this.active = active;
        return this;
    }

    public String getExpirationTime() {
        return expirationTime;
    }

    public FederationServerUpdateParams setExpirationTime(String expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public String getSecurityKey() {
        return securityKey;
    }

    public FederationServerUpdateParams setSecurityKey(String securityKey) {
        this.securityKey = securityKey;
        return this;
    }
}
