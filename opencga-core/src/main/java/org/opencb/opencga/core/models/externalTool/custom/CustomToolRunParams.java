package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class CustomToolRunParams extends ToolParams  {

    public static final String DESCRIPTION = "Custom tool run parameters";

    @DataField(id = "commandLine", description = "Command line to be executed inside the container.")
    private String commandLine;

    @DataField(id = "params", description = "Key-value pairs of parameters to be used inside the command line.")
    private Map<String, String> params;

    public CustomToolRunParams() {
    }

    public CustomToolRunParams(CustomToolRunParams other) {
        this.commandLine = other.commandLine;
        this.params = other.params;
    }

    public CustomToolRunParams(String commandLine, Map<String, String> params) {
        this.commandLine = commandLine;
        this.params = params;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolRunParams{");
        sb.append("commandLine='").append(commandLine).append('\'');
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
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
