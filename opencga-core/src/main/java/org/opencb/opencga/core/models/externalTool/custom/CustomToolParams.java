package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.externalTool.Docker;
import org.opencb.opencga.core.models.externalTool.ExternalToolParams;

public class CustomToolParams extends ExternalToolParams<CustomToolRunParams> {

    @DataField(id = "docker", description = "Docker to be used.")
    private Docker docker;

    public CustomToolParams() {
    }

    public CustomToolParams(Docker docker) {
        this.docker = docker;
    }

    public CustomToolParams(String id, Integer version, CustomToolRunParams params) {
        super(id, version, params);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolParams{");
        sb.append("docker=").append(docker);
        sb.append(", id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public Docker getDocker() {
        return docker;
    }

    public CustomToolParams setDocker(Docker docker) {
        this.docker = docker;
        return this;
    }
}
