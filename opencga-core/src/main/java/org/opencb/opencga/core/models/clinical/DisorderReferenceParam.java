package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.Disorder;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class DisorderReferenceParam {

    @DataField(description = ParamConstants.DISORDER_REFERENCE_PARAM_ID_DESCRIPTION)
    private String id;

    public DisorderReferenceParam() {
    }

    public DisorderReferenceParam(String id) {
        this.id = id;
    }

    public static DisorderReferenceParam of(Disorder disorder) {
        return new DisorderReferenceParam(disorder.getId());
    }

    public Disorder toDisorder() {
        return new Disorder().setId(id);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DisorderReferenceParam{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public DisorderReferenceParam setId(String id) {
        this.id = id;
        return this;
    }
}
