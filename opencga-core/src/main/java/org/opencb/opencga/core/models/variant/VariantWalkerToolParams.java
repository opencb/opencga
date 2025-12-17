package org.opencb.opencga.core.models.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.externalTool.ExternalToolParams;
import org.opencb.opencga.core.tools.ToolParams;

public class VariantWalkerToolParams extends ToolParams implements ExternalToolParams {

    public static final String DESCRIPTION = "Variant walker tool run parameters";

    @DataField(id = "id", description = "User tool identifier.")
    private String id;

    @DataField(id = "version", description = "User tool version. If not provided, the latest version will be used.")
    private Integer version;

    @DataField(id = "params", description = "Variant walker params.")
    private VariantWalkerParams params;

    public VariantWalkerToolParams() {
    }

    public VariantWalkerToolParams(String id, Integer version, VariantWalkerParams params) {
        this.id = id;
        this.version = version;
        this.params = params;
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

    @Override
    public String getId() {
        return id;
    }

    public VariantWalkerToolParams setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public Integer getVersion() {
        return version;
    }

    public VariantWalkerToolParams setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public VariantWalkerParams getParams() {
        return params;
    }

    public VariantWalkerToolParams setParams(VariantWalkerParams params) {
        this.params = params;
        return this;
    }
}
