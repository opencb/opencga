package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

public abstract class ExternalToolParams<TOOL_PARAMS extends ToolParams> extends ToolParams {

    @DataField(id = "id", description = "External tool identifier.")
    protected String id;

    @DataField(id = "version", description = "External tool version. If not provided, the latest version will be used.")
    protected Integer version;

    @DataField(id = "params", description = "External tool specific parameters.")
    protected TOOL_PARAMS params;

    public ExternalToolParams() {
    }

    public ExternalToolParams(String id, Integer version, TOOL_PARAMS params) {
        this.id = id;
        this.version = version;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalToolParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExternalToolParams<TOOL_PARAMS> setId(String id) {
        this.id = id;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public ExternalToolParams<TOOL_PARAMS> setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public TOOL_PARAMS getParams() {
        return params;
    }

    public ExternalToolParams<TOOL_PARAMS> setParams(TOOL_PARAMS params) {
        this.params = params;
        return this;
    }

}
