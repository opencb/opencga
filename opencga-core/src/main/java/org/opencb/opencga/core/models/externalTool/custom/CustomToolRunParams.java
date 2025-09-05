package org.opencb.opencga.core.models.externalTool.custom;

import java.util.Map;

public class CustomToolRunParams {

    private String commandLine;
    private Map<String, String> params;

    public CustomToolRunParams() {
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
