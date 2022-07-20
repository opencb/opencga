package org.opencb.opencga.core.models.individual;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class IndividualReferenceParam {

    @DataField(description = ParamConstants.INDIVIDUAL_REFERENCE_PARAM_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.INDIVIDUAL_REFERENCE_PARAM_ID_DESCRIPTION)
    private String uuid;

    public IndividualReferenceParam() {
    }

    public IndividualReferenceParam(String id, String uuid) {
        this.id = id;
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualReferenceParam{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public IndividualReferenceParam setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public IndividualReferenceParam setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
