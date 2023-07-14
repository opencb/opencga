package org.opencb.opencga.core.models.organizations;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.models.PrivateFields;
import org.opencb.opencga.core.models.project.Project;

import java.util.List;
import java.util.Map;

@DataClass(id = "Organization", since = "3.0",
        description = "Organization data model hosts information about the organization managing the data.")
public class Organization extends PrivateFields {

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.ORGANIZATION_ID_DESCRIPTION)
    private String id;

    @DataField(id = "name", description = FieldConstants.ORGANIZATION_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "domain", description = FieldConstants.ORGANIZATION_DOMAIN_DESCRIPTION)
    private String domain;

    @DataField(id = "owner", description = FieldConstants.ORGANIZATION_OWNER_DESCRIPTION)
    private String owner;

    @DataField(id = "admins", description = FieldConstants.ORGANIZATION_ADMINS_DESCRIPTION)
    private List<String> admins;

    @DataField(id = "creationDate", description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "projects", description = FieldConstants.ORGANIZATION_PROJECTS_DESCRIPTION)
    private List<Project> projects;

    @DataField(id = "authenticationOrigins", description = FieldConstants.ORGANIZATION_AUTHENTICATION_ORIGINS_DESCRIPTION)
    private List<AuthenticationOrigin> authenticationOrigins;

    @DataField(id = "attributes",  description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Organization() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Organization{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", domain='").append(domain).append('\'');
        sb.append(", owner='").append(owner).append('\'');
        sb.append(", admins=").append(admins);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", projects=").append(projects);
        sb.append(", authenticationOrigins=").append(authenticationOrigins);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getUuid() {
        return uuid;
    }

    public Organization setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getId() {
        return id;
    }

    public Organization setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Organization setName(String name) {
        this.name = name;
        return this;
    }

    public String getDomain() {
        return domain;
    }

    public Organization setDomain(String domain) {
        this.domain = domain;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public Organization setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public List<String> getAdmins() {
        return admins;
    }

    public Organization setAdmins(List<String> admins) {
        this.admins = admins;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Organization setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Organization setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public Organization setProjects(List<Project> projects) {
        this.projects = projects;
        return this;
    }

    public List<AuthenticationOrigin> getAuthenticationOrigins() {
        return authenticationOrigins;
    }

    public Organization setAuthenticationOrigins(List<AuthenticationOrigin> authenticationOrigins) {
        this.authenticationOrigins = authenticationOrigins;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Organization setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
