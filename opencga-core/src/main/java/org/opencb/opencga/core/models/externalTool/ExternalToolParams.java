package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

public class ExternalToolParams<TOOL_PARAMS extends ToolParams> extends ToolParams {

    public static final String DESCRIPTION = "External tool run parameters";

    @DataField(id = "study", description = "Study FQN where the tool is registered.")
    private String study;

    @DataField(id = "id", description = "External tool identifier.")
    private String id;

    @DataField(id = "version", description = "External tool version. If not provided, the latest version will be used.")
    private Integer version;

    @DataField(id = "docker", description = "Docker to be used.")
    private Docker docker;

    @DataField(id = "params", description = "External tool specific parameters.")
    private TOOL_PARAMS params;

    public ExternalToolParams() {
    }

    public ExternalToolParams(String study, String id, Integer version, TOOL_PARAMS params) {
        this.study = study;
        this.id = id;
        this.version = version;
        this.params = params;
    }

    public ExternalToolParams(Docker docker) {
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalToolParams{");
        sb.append("study='").append(study).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", docker=").append(docker);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public ExternalToolParams<TOOL_PARAMS> setStudy(String study) {
        this.study = study;
        return this;
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

    public Docker getDocker() {
        return docker;
    }

    public ExternalToolParams<TOOL_PARAMS> setDocker(Docker docker) {
        this.docker = docker;
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
