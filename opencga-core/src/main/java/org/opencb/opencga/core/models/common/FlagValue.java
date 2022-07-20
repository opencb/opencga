package org.opencb.opencga.core.models.common;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FlagValue {

    @DataField(description = ParamConstants.FLAG_VALUE_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    public FlagValue() {
    }

    public FlagValue(String id) {
        this.id = id;
    }

    public FlagValue(String id, String description) {
        this.id = id;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FlagValue{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FlagValue setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FlagValue setDescription(String description) {
        this.description = description;
        return this;
    }
}
