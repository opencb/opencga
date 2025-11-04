package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.opencga.core.models.externalTool.Docker;
import org.opencb.opencga.core.models.job.MinimumRequirements;

public class CustomToolInlineParams {

    private Docker container;
    private MinimumRequirements minimumRequirements;

    public CustomToolInlineParams() {
    }

    public CustomToolInlineParams(Docker container, MinimumRequirements minimumRequirements) {
        this.container = container;
        this.minimumRequirements = minimumRequirements;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolInlineParams{");
        sb.append("container=").append(container);
        sb.append(", minimumRequirements=").append(minimumRequirements);
        sb.append('}');
        return sb.toString();
    }

    public Docker getContainer() {
        return container;
    }

    public CustomToolInlineParams setContainer(Docker container) {
        this.container = container;
        return this;
    }

    public MinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public CustomToolInlineParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }
}
