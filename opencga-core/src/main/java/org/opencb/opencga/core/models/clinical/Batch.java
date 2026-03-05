package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class Batch {

    @DataField(id = "id", indexed = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", indexed = true,
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    public Batch() {
    }

    public Batch(String id, String description) {
        this.id = id;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Batch{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Batch setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Batch setDescription(String description) {
        this.description = description;
        return this;
    }
}
