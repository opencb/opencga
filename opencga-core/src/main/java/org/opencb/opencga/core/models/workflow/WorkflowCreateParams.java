package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;
import java.util.Map;

public class WorkflowCreateParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.WORKFLOW_ID_DESCRIPTION)
    private String id;

    @DataField(id = "name", description = FieldConstants.WORKFLOW_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "manager", description = FieldConstants.WORKFLOW_MANAGER_DESCRIPTION)
    private WorkflowSystem manager;

    @DataField(id = "type", description = FieldConstants.WORKFLOW_TYPE_DESCRIPTION)
    private Workflow.Type type;

    @DataField(id = "tags", description = FieldConstants.WORKFLOW_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "draft", description = FieldConstants.WORKFLOW_DRAFT_DESCRIPTION)
    private boolean draft;

    @DataField(id = "repository", description = FieldConstants.WORKFLOW_REPOSITORY_DESCRIPTION)
    private WorkflowRepository repository;

    @DataField(id = "variables", description = FieldConstants.WORKFLOW_VARIABLES_DESCRIPTION)
    private List<WorkflowVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.WORKFLOW_MINIMUM_REQUIREMENTS_DESCRIPTION)
    private WorkflowMinimumRequirements minimumRequirements;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<WorkflowScript> scripts;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public WorkflowCreateParams() {
    }

    public WorkflowCreateParams(String id, String name, String description, WorkflowSystem manager, Workflow.Type type, List<String> tags,
                                boolean draft, WorkflowRepository repository, List<WorkflowVariable> variables,
                                WorkflowMinimumRequirements minimumRequirements, List<WorkflowScript> scripts, String creationDate,
                                String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.manager = manager;
        this.type = type;
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
        sb.append(", type=").append(type);
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

    public Workflow toWorkflow() {
        return new Workflow(id, name, description, type, manager, tags, variables, minimumRequirements, draft, repository, scripts,
                new WorkflowInternal(), creationDate, modificationDate, attributes);
    }

    public String getId() {
        return id;
    }

    public WorkflowCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public WorkflowCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public WorkflowCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public WorkflowCreateParams setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public Workflow.Type getType() {
        return type;
    }

    public WorkflowCreateParams setType(Workflow.Type type) {
        this.type = type;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public WorkflowCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public WorkflowCreateParams setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public WorkflowCreateParams setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

    public List<WorkflowVariable> getVariables() {
        return variables;
    }

    public WorkflowCreateParams setVariables(List<WorkflowVariable> variables) {
        this.variables = variables;
        return this;
    }

    public WorkflowMinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public WorkflowCreateParams setMinimumRequirements(WorkflowMinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public WorkflowCreateParams setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public WorkflowCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public WorkflowCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public WorkflowCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}