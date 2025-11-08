package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.externalTool.ExternalToolParams;

public class CustomToolParams extends ExternalToolParams<CustomToolRunParams> {

    @DataField(id = "container", description = "Docker container to be used.")
    private Container container;

    public CustomToolParams() {
    }

    public CustomToolParams(Container container) {
        this.container = container;
    }

    public CustomToolParams(String id, Integer version, CustomToolRunParams params) {
        super(id, version, params);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolParams{");
        sb.append("container=").append(container);
        sb.append(", id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", params=").append(params);
        sb.append('}');
        return sb.toString();
    }

    public Container getContainer() {
        return container;
    }

    public CustomToolParams setContainer(Container container) {
        this.container = container;
        return this;
    }
}
