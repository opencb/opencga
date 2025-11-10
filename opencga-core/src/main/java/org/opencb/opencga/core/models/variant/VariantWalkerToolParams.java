package org.opencb.opencga.core.models.variant;

import org.opencb.opencga.core.models.externalTool.ExternalToolParams;

public class VariantWalkerToolParams extends ExternalToolParams<VariantWalkerParams> {

    public static final String DESCRIPTION = "Variant walker tool run parameters";

    public VariantWalkerToolParams() {
    }

    public VariantWalkerToolParams(String id, Integer version, VariantWalkerParams params) {
        super(id, version, params);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantWalkerToolParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

}
