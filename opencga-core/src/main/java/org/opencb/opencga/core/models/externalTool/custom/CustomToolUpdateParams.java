package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.externalTool.ExternalToolScope;
import org.opencb.opencga.core.models.externalTool.ExternalToolUpdateParams;
import org.opencb.opencga.core.models.externalTool.ExternalToolVariable;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

public class CustomToolUpdateParams extends ExternalToolUpdateParams {

    @DataField(id = "container", description = FieldConstants.EXTERNAL_TOOL_CONTAINER_DESCRIPTION)
    private Container container;

    public CustomToolUpdateParams() {
    }

    public CustomToolUpdateParams(String name, String description, ExternalToolScope scope, List<String> tags,
                                  List<ExternalToolVariable> variables, MinimumRequirements minimumRequirements, boolean draft,
                                  String creationDate, String modificationDate, Map<String, Object> attributes, Container container) {
        super(name, description, scope, tags, variables, minimumRequirements, draft, creationDate, modificationDate, attributes);
        this.container = container;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolUpdateParams{");
        sb.append(super.toString());
        sb.append(", container=").append(container);
        sb.append('}');
        return sb.toString();
    }

    public Container getContainer() {
        return container;
    }

    public CustomToolUpdateParams setContainer(Container container) {
        this.container = container;
        return this;
    }

    @Override
    public CustomToolUpdateParams setName(String name) {
        super.setName(name);
        return this;
    }

    @Override
    public CustomToolUpdateParams setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    @Override
    public CustomToolUpdateParams setScope(ExternalToolScope scope) {
        super.setScope(scope);
        return this;
    }

    @Override
    public CustomToolUpdateParams setTags(List<String> tags) {
        super.setTags(tags);
        return this;
    }

    @Override
    public CustomToolUpdateParams setVariables(List<ExternalToolVariable> variables) {
        super.setVariables(variables);
        return this;
    }

    @Override
    public CustomToolUpdateParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        super.setMinimumRequirements(minimumRequirements);
        return this;
    }

    @Override
    public CustomToolUpdateParams setDraft(boolean draft) {
        super.setDraft(draft);
        return this;
    }

    @Override
    public CustomToolUpdateParams setCreationDate(String creationDate) {
        super.setCreationDate(creationDate);
        return this;
    }

    @Override
    public CustomToolUpdateParams setModificationDate(String modificationDate) {
        super.setModificationDate(modificationDate);
        return this;
    }

    @Override
    public CustomToolUpdateParams setAttributes(Map<String, Object> attributes) {
        super.setAttributes(attributes);
        return this;
    }
}
