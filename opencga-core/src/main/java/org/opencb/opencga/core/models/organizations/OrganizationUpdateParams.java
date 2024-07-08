package org.opencb.opencga.core.models.organizations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public final class OrganizationUpdateParams {

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

    @DataField(id = "attributes",  description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public OrganizationUpdateParams() {
    }

    public OrganizationUpdateParams(String name, String owner, List<String> admins, String creationDate, String modificationDate,
                                    Map<String, Object> attributes) {
        this.name = name;
        this.owner = owner;
        this.admins = admins;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", owner='").append(owner).append('\'');
        sb.append(", admins=").append(admins);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public OrganizationUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public OrganizationUpdateParams setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public List<String> getAdmins() {
        return admins;
    }

    public OrganizationUpdateParams setAdmins(List<String> admins) {
        this.admins = admins;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public OrganizationUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public OrganizationUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public OrganizationUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
