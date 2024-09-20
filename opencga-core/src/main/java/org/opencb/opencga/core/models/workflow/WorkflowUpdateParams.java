package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.study.Variable;

import java.util.List;
import java.util.Map;

public class WorkflowUpdateParams {

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

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<WorkflowScript> scripts;

    @DataField(id = "variables", description = FieldConstants.WORKFLOW_VARIABLES_DESCRIPTION)
    private List<Variable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.WORKFLOW_MINIMUM_REQUIREMENTS_DESCRIPTION)
    private WorkflowMinimumRequirements minimumRequirements;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public WorkflowUpdateParams() {
    }

    public WorkflowUpdateParams(String name, String description, WorkflowSystem manager, Workflow.Type type, List<String> tags,
                                boolean draft, WorkflowRepository repository, List<WorkflowScript> scripts, List<Variable> variables,
                                WorkflowMinimumRequirements minimumRequirements, String creationDate, String modificationDate,
                                Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.manager = manager;
        this.type = type;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", manager=").append(manager);
        sb.append(", type=").append(type);
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

    public WorkflowUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public WorkflowUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public WorkflowUpdateParams setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public Workflow.Type getType() {
        return type;
    }

    public WorkflowUpdateParams setType(Workflow.Type type) {
        this.type = type;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public WorkflowUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public WorkflowUpdateParams setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public WorkflowUpdateParams setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public WorkflowUpdateParams setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public List<Variable> getVariables() {
        return variables;
    }

    public WorkflowUpdateParams setVariables(List<Variable> variables) {
        this.variables = variables;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public WorkflowUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public WorkflowUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public WorkflowUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
