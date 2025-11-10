package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.opencga.core.models.externalTool.ExternalToolParams;

public class CustomExternalToolParams  extends ExternalToolParams<CustomToolRunParams> {

    public CustomExternalToolParams() {
    }

    public CustomExternalToolParams(String id, Integer version, CustomToolRunParams params) {
        super(id, version, params);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }
}
