package org.opencb.opencga.core.models.federation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FederationClientParamsRef {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.FEDERATION_CLIENT_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.FEDERATION_CLIENT_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "version", description = FieldConstants.FEDERATION_CLIENT_VERSION_DESCRIPTION)
    private String version;

    public FederationClientParamsRef() {
    }

    public FederationClientParamsRef(String id) {
        this(id, "", "");
    }

    public FederationClientParamsRef(String id, String description, String version) {
        this.id = id;
        this.description = description;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationClientRef{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
