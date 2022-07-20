package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalStatusValue {

    @DataField(description = ParamConstants.CLINICAL_STATUS_VALUE_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.CLINICAL_STATUS_VALUE_TYPE_DESCRIPTION)
    private ClinicalStatusType type;

    public enum ClinicalStatusType {
        NOT_STARTED,
        IN_PROGRESS,
        CLOSED,
        UNKNOWN
    }

    public ClinicalStatusValue() {
    }

    public ClinicalStatusValue(String id, String description, ClinicalStatusType type) {
        this.id = id;
        this.description = description;
        this.type = type;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalStatusValue{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalStatusValue setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalStatusValue setDescription(String description) {
        this.description = description;
        return this;
    }

    public ClinicalStatusType getType() {
        return type;
    }

    public ClinicalStatusValue setType(ClinicalStatusType type) {
        this.type = type;
        return this;
    }
}
