package org.opencb.opencga.core.models.externalTool.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.externalTool.ExternalToolScope;
import org.opencb.opencga.core.models.externalTool.ExternalToolUpdateParams;
import org.opencb.opencga.core.models.externalTool.ExternalToolVariable;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

public class WorkflowUpdateParams extends ExternalToolUpdateParams {

    @DataField(id = "workflow", description = FieldConstants.EXTERNAL_TOOL_WORKFLOW_DESCRIPTION)
    private Workflow workflow;

    public WorkflowUpdateParams() {
    }

    public WorkflowUpdateParams(String name, String description, ExternalToolScope scope, List<String> tags,
                                List<ExternalToolVariable> variables, MinimumRequirements minimumRequirements, boolean draft,
                                String creationDate, String modificationDate, Map<String, Object> attributes, Workflow workflow) {
        super(name, description, scope, tags, variables, minimumRequirements, draft, creationDate, modificationDate, attributes);
        this.workflow = workflow;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowUpdateParams{");
        sb.append(super.toString());
        sb.append(", workflow=").append(workflow);
        sb.append('}');
        return sb.toString();
    }

    public Workflow getWorkflow() {
        return workflow;
    }

    public WorkflowUpdateParams setWorkflow(Workflow workflow) {
        this.workflow = workflow;
        return this;
    }

    @Override
    public WorkflowUpdateParams setName(String name) {
        super.setName(name);
        return this;
    }

    @Override
    public WorkflowUpdateParams setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    @Override
    public WorkflowUpdateParams setScope(ExternalToolScope scope) {
        super.setScope(scope);
        return this;
    }

    @Override
    public WorkflowUpdateParams setTags(List<String> tags) {
        super.setTags(tags);
        return this;
    }

    @Override
    public WorkflowUpdateParams setVariables(List<ExternalToolVariable> variables) {
        super.setVariables(variables);
        return this;
    }

    @Override
    public WorkflowUpdateParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        super.setMinimumRequirements(minimumRequirements);
        return this;
    }

    @Override
    public WorkflowUpdateParams setDraft(boolean draft) {
        super.setDraft(draft);
        return this;
    }

    @Override
    public WorkflowUpdateParams setCreationDate(String creationDate) {
        super.setCreationDate(creationDate);
        return this;
    }

    @Override
    public WorkflowUpdateParams setModificationDate(String modificationDate) {
        super.setModificationDate(modificationDate);
        return this;
    }

    @Override
    public WorkflowUpdateParams setAttributes(Map<String, Object> attributes) {
        super.setAttributes(attributes);
        return this;
    }
}
