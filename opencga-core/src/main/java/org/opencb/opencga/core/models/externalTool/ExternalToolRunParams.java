package org.opencb.opencga.core.models.externalTool;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class ExternalToolRunParams extends ToolParams {

    public static final String DESCRIPTION = "External tool run parameters";

    private String id;
    private Integer version;
    private Map<String, String> params;
    private String commandLine;

    private Docker docker;

    public ExternalToolRunParams() {
    }

    public ExternalToolRunParams(String id, Integer version, Map<String, String> params) {
        this.id = id;
        this.version = version;
        this.params = params;
    }

    public ExternalToolRunParams(String id, Integer version, Map<String, String> params, String commandLine) {
        this.id = id;
        this.version = version;
        this.params = params;
        this.commandLine = commandLine;
    }

    public ExternalToolRunParams(Docker docker) {
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalToolRunParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", docker=").append(docker);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExternalToolRunParams setId(String id) {
        this.id = id;
        return this;
    }

    public Integer getVersion() {
        return version;
    }

    public ExternalToolRunParams setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public ExternalToolRunParams setParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public ExternalToolRunParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public Docker getDocker() {
        return docker;
    }

    public ExternalToolRunParams setDocker(Docker docker) {
        this.docker = docker;
        return this;
    }
}
