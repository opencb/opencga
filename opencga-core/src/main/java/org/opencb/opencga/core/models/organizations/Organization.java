package org.opencb.opencga.core.models.organizations;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.federation.Federation;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.project.Project;

import java.util.List;
import java.util.Map;

@DataClass(id = "Organization", since = "3.0",
        description = "Organization data model hosts information about the organization managing the data.")
public class Organization {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.ORGANIZATION_ID_DESCRIPTION)
    private String id;

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "name", description = FieldConstants.ORGANIZATION_NAME_DESCRIPTION)
    private String name;

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

    @DataField(id = "federation", description = FieldConstants.ORGANIZATION_FEDERATION_DESCRIPTION)
    private Federation federation;

    @DataField(id = "notes", description = FieldConstants.ORGANIZATION_NOTES_DESCRIPTION)
    private List<Note> notes;

    @DataField(id = "configuration", description = FieldConstants.ORGANIZATION_CONFIGURATION_DESCRIPTION)
    private OrganizationConfiguration configuration;

    @DataField(id = "internal", managed = true, description = FieldConstants.ORGANIZATION_INTERNAL_DESCRIPTION)
    private OrganizationInternal internal;

    @DataField(id = "attributes",  description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public Organization() {
    }

    public Organization(String id, String name, String owner, List<String> admins, String creationDate, String modificationDate,
                        List<Project> projects, OrganizationConfiguration configuration, OrganizationInternal internal,
                        Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.owner = owner;
        this.admins = admins;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.projects = projects;
        this.configuration = configuration;
        this.internal = internal;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Organization{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", owner='").append(owner).append('\'');
        sb.append(", admins=").append(admins);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", projects=").append(projects);
        sb.append(", federation=").append(federation);
        sb.append(", notes=").append(notes);
        sb.append(", configuration=").append(configuration);
        sb.append(", internal=").append(internal);
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

    public Federation getFederation() {
        return federation;
    }

    public Organization setFederation(Federation federation) {
        this.federation = federation;
        return this;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public Organization setProjects(List<Project> projects) {
        this.projects = projects;
        return this;
    }

    public List<Note> getNotes() {
        return notes;
    }

    public Organization setNotes(List<Note> notes) {
        this.notes = notes;
        return this;
    }

    public OrganizationConfiguration getConfiguration() {
        return configuration;
    }

    public Organization setConfiguration(OrganizationConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public OrganizationInternal getInternal() {
        return internal;
    }

    public Organization setInternal(OrganizationInternal internal) {
        this.internal = internal;
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
