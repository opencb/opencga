package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

public class ExternalToolCreateParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.WORKFLOW_ID_DESCRIPTION)
    private String id;

    @DataField(id = "name", description = FieldConstants.WORKFLOW_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "manager", description = FieldConstants.WORKFLOW_MANAGER_DESCRIPTION)
    private WorkflowSystem manager;

    @DataField(id = "scope", description = FieldConstants.WORKFLOW_SCOPE_DESCRIPTION)
    private ExternalTool.Scope scope;

    @DataField(id = "tags", description = FieldConstants.WORKFLOW_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "draft", description = FieldConstants.WORKFLOW_DRAFT_DESCRIPTION)
    private boolean draft;

    @DataField(id = "repository", description = FieldConstants.WORKFLOW_REPOSITORY_DESCRIPTION)
    private WorkflowRepository repository;

    @DataField(id = "variables", description = FieldConstants.WORKFLOW_VARIABLES_DESCRIPTION)
    private List<ExternalToolVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.MINIMUM_REQUIREMENTS_DESCRIPTION)
    private MinimumRequirements minimumRequirements;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<WorkflowScript> scripts;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public ExternalToolCreateParams() {
    }

    public ExternalToolCreateParams(String id, String name, String description, WorkflowSystem manager, ExternalTool.Scope scope, List<String> tags,
                                    boolean draft, WorkflowRepository repository, List<ExternalToolVariable> variables,
                                    MinimumRequirements minimumRequirements, List<WorkflowScript> scripts, String creationDate,
                                    String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.manager = manager;
        this.scope = scope;
        this.tags = tags;
        this.draft = draft;
        this.repository = repository;
        this.variables = variables;
        this.minimumRequirements = minimumRequirements;
        this.scripts = scripts;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", manager=").append(manager);
        sb.append(", scope=").append(scope);
        sb.append(", tags=").append(tags);
        sb.append(", draft=").append(draft);
        sb.append(", repository=").append(repository);
        sb.append(", variables=").append(variables);
        sb.append(", minimumRequirements=").append(minimumRequirements);
        sb.append(", scripts=").append(scripts);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public ExternalTool toWorkflow() {
        return new ExternalTool(id, name, description, scope, manager, tags, variables, minimumRequirements, draft, repository, scripts,
                new ExternalToolInternal(), creationDate, modificationDate, attributes);
    }

    public String getId() {
        return id;
    }

    public ExternalToolCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ExternalToolCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExternalToolCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public ExternalToolCreateParams setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public ExternalTool.Scope getScope() {
        return scope;
    }

    public ExternalToolCreateParams setScope(ExternalTool.Scope scope) {
        this.scope = scope;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public ExternalToolCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public ExternalToolCreateParams setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public ExternalToolCreateParams setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

    public List<ExternalToolVariable> getVariables() {
        return variables;
    }

    public ExternalToolCreateParams setVariables(List<ExternalToolVariable> variables) {
        this.variables = variables;
        return this;
    }

    public MinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public ExternalToolCreateParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public ExternalToolCreateParams setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ExternalToolCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ExternalToolCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ExternalToolCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
