package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.job.MinimumRequirements;
import org.opencb.opencga.core.tools.ToolParams;

public class  CustomToolInlineParams extends ToolParams {

    @DataField(id = "container", description = "Docker container to be used.")
    private Container container;

    @DataField(id = "minimumRequirements", description = "Minimum requirements for the tool execution.")
    private MinimumRequirements minimumRequirements;

    public CustomToolInlineParams() {
    }

    public CustomToolInlineParams(Container container, MinimumRequirements minimumRequirements) {
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

    public Container getContainer() {
        return container;
    }

    public CustomToolInlineParams setContainer(Container container) {
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
