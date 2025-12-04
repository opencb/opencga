package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.externalTool.ExternalToolParams;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class CustomToolRunParams extends ToolParams implements ExternalToolParams {

    public static final String DESCRIPTION = "Custom tool run parameters";

    @DataField(id = "id", description = "User tool identifier.")
    private String id;

    @DataField(id = "version", description = "User tool version. If not provided, the latest version will be used.")
    private Integer version;

    @DataField(id = "commandLine", description = FieldConstants.CONTAINER_COMMANDLINE_DESCRIPTION)
    private String commandLine;

    @DataField(id = "params", description = "Key-value pairs of parameters to be used inside the command line.")
    private Map<String, String> params;

    public CustomToolRunParams() {
    }

    public CustomToolRunParams(String id, Integer version, String commandLine, Map<String, String> params) {
        this.id = id;
        this.version = version;
        this.commandLine = commandLine;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolRunParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CustomToolRunParams setId(String id) {
        this.id = id;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public CustomToolRunParams setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public CustomToolRunParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public CustomToolRunParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }
}
