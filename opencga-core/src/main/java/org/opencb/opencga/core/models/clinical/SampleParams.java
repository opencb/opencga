package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.sample.Sample;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleParams {
    @DataField(description = ParamConstants.SAMPLE_PARAMS_ID_DESCRIPTION)
    private String id;

    public SampleParams() {
    }

    public SampleParams(String id) {
        this.id = id;
    }

    public static SampleParams of(Sample sample) {
        return new SampleParams(sample.getId());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleParams{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleParams setId(String id) {
        this.id = id;
        return this;
    }
}
