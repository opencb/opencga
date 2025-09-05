package org.opencb.opencga.core.models.externalTool.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

@Deprecated
public class DeprecatedWorkflowUpdateParams {

    @DataField(id = "name", description = FieldConstants.EXTERNAL_TOOL_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "manager", description = FieldConstants.WORKFLOW_MANAGER_DESCRIPTION)
    private WorkflowSystem manager;

    @DataField(id = "scope", description = FieldConstants.EXTERNAL_TOOL_SCOPE_DESCRIPTION)
    private ExternalToolScope scope;

    @DataField(id = "tags", description = FieldConstants.EXTERNAL_TOOL_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "draft", description = FieldConstants.EXTERNAL_TOOL_DRAFT_DESCRIPTION)
    private boolean draft;

    @DataField(id = "repository", description = FieldConstants.WORKFLOW_REPOSITORY_DESCRIPTION)
    private WorkflowRepository repository;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<WorkflowScript> scripts;

    @DataField(id = "variables", description = FieldConstants.EXTERNAL_TOOL_VARIABLES_DESCRIPTION)
    private List<ExternalToolVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.MINIMUM_REQUIREMENTS_DESCRIPTION)
    private MinimumRequirements minimumRequirements;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public DeprecatedWorkflowUpdateParams() {
    }

    public DeprecatedWorkflowUpdateParams(String name, String description, WorkflowSystem manager, ExternalToolScope scope, List<String> tags,
                                          boolean draft, WorkflowRepository repository, List<WorkflowScript> scripts,
                                          List<ExternalToolVariable> variables, MinimumRequirements minimumRequirements, String creationDate,
                                          String modificationDate, Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.manager = manager;
        this.scope = scope;
        this.tags = tags;
        this.draft = draft;
        this.repository = repository;
        this.scripts = scripts;
        this.variables = variables;
        this.minimumRequirements = minimumRequirements;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    public WorkflowUpdateParams toWorkflowUpdateParams() {
        Workflow workflow = null;
        if (manager != null || scripts != null || repository != null) {
            workflow = new Workflow(manager, scripts, repository);
        }
        return new WorkflowUpdateParams(name, description, scope, tags, variables, minimumRequirements, draft, creationDate,
                modificationDate, attributes, workflow);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", manager=").append(manager);
        sb.append(", scope=").append(scope);
        sb.append(", tags=").append(tags);
        sb.append(", draft=").append(draft);
        sb.append(", repository=").append(repository);
        sb.append(", scripts=").append(scripts);
        sb.append(", variables=").append(variables);
        sb.append(", minimumRequirements=").append(minimumRequirements);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public DeprecatedWorkflowUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DeprecatedWorkflowUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public DeprecatedWorkflowUpdateParams setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public ExternalToolScope getScope() {
        return scope;
    }

    public DeprecatedWorkflowUpdateParams setScope(ExternalToolScope scope) {
        this.scope = scope;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public DeprecatedWorkflowUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public DeprecatedWorkflowUpdateParams setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public DeprecatedWorkflowUpdateParams setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public DeprecatedWorkflowUpdateParams setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public List<ExternalToolVariable> getVariables() {
        return variables;
    }

    public DeprecatedWorkflowUpdateParams setVariables(List<ExternalToolVariable> variables) {
        this.variables = variables;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public DeprecatedWorkflowUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public DeprecatedWorkflowUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public MinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public DeprecatedWorkflowUpdateParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public DeprecatedWorkflowUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
