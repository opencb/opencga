package org.opencb.opencga.core.models.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.externalTool.ExternalToolParams;

public class VariantWalkerToolParams extends ExternalToolParams<VariantWalkerParams> {

    public static final String DESCRIPTION = "Variant walker tool run parameters";

    @DataField(id = "study", description = "Study FQN where the tool is registered.")
    private String study;

    public VariantWalkerToolParams() {
    }

    public VariantWalkerToolParams(String study, String id, Integer version, VariantWalkerParams params) {
        super(id, version, params);
        this.study = study;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantWalkerToolParams{");
        sb.append("study='").append(study).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public VariantWalkerToolParams setStudy(String study) {
        this.study = study;
        return this;
    }
}
