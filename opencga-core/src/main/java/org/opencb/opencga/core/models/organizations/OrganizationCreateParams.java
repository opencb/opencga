package org.opencb.opencga.core.models.organizations;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;

import java.util.List;
import java.util.Map;

public class OrganizationCreateParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.ORGANIZATION_ID_DESCRIPTION)
    private String id;

    @DataField(id = "name", description = FieldConstants.ORGANIZATION_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "domain", description = FieldConstants.ORGANIZATION_DOMAIN_DESCRIPTION)
    private String domain;

    @DataField(id = "creationDate", description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "authenticationOrigins", description = FieldConstants.ORGANIZATION_AUTHENTICATION_ORIGINS_DESCRIPTION)
    private List<AuthenticationOrigin> authenticationOrigins;

    @DataField(id = "attributes",  description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public OrganizationCreateParams() {
    }

    public OrganizationCreateParams(String id, String name, String domain, String creationDate, String modificationDate,
                                    List<AuthenticationOrigin> authenticationOrigins, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.domain = domain;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.authenticationOrigins = authenticationOrigins;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", domain='").append(domain).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", authenticationOrigins=").append(authenticationOrigins);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public static OrganizationCreateParams of(Organization organization) {
        return new OrganizationCreateParams(organization.getId(), organization.getName(), organization.getDomain(),
                organization.getCreationDate(), organization.getModificationDate(),organization.getAuthenticationOrigins(),
                organization.getAttributes());
    }

    public Organization toOrganization() {
        return new Organization(id, name, domain, null, null, creationDate, modificationDate, null, authenticationOrigins, attributes);
    }

    public String getId() {
        return id;
    }

    public OrganizationCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public OrganizationCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDomain() {
        return domain;
    }

    public OrganizationCreateParams setDomain(String domain) {
        this.domain = domain;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public OrganizationCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public OrganizationCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<AuthenticationOrigin> getAuthenticationOrigins() {
        return authenticationOrigins;
    }

    public OrganizationCreateParams setAuthenticationOrigins(List<AuthenticationOrigin> authenticationOrigins) {
        this.authenticationOrigins = authenticationOrigins;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public OrganizationCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
