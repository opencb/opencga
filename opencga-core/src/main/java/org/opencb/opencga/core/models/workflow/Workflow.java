package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;

import java.util.List;
import java.util.Map;

@DataClass(id = "Workflow", since = "3.3.0", description = "Job data model hosts information about any job.")
public class Workflow extends PrivateStudyUid {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.WORKFLOW_ID_DESCRIPTION)
    private String id;

    @DataField(id = "uuid", managed = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "name", description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String name;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "type", description = FieldConstants.WORKFLOW_TYPE_DESCRIPTION)
    private Type type;

    @DataField(id = "manager", description = FieldConstants.WORKFLOW_MANAGER_DESCRIPTION)
    private WorkflowSystem manager;

    @DataField(id = "tags", description = FieldConstants.WORKFLOW_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "variables", description = FieldConstants.WORKFLOW_VARIABLES_DESCRIPTION)
    private List<WorkflowVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.WORKFLOW_MINIMUM_REQUIREMENTS_DESCRIPTION)
    private WorkflowMinimumRequirements minimumRequirements;

    @DataField(id = "draft", description = FieldConstants.WORKFLOW_DRAFT_DESCRIPTION)
    private boolean draft;

    @DataField(id = "repository", description = FieldConstants.WORKFLOW_REPOSITORY_DESCRIPTION)
    private WorkflowRepository repository;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<WorkflowScript> scripts;

    @DataField(id = "version", managed = true, indexed = true, description = FieldConstants.GENERIC_VERSION_DESCRIPTION)
    private int version;

    @DataField(id = "release", managed = true, indexed = true, description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    @DataField(id = "internal", description = FieldConstants.WORKFLOW_INTERNAL_DESCRIPTION)
    private WorkflowInternal internal;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public enum Type {
        SECONDARY_ANALYSIS,
        RESEARCH_ANALYSIS,
        CLINICAL_INTERPRETATION_ANALYSIS,
        OTHER
    }

    public Workflow() {
    }

    public Workflow(String id, String name, String description, Type type, WorkflowSystem manager, List<String> tags,
                    List<WorkflowVariable> variables, WorkflowMinimumRequirements minimumRequirements, boolean draft,
                    WorkflowRepository repository, List<WorkflowScript> scripts, WorkflowInternal internal, String creationDate,
                    String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.draft = draft;
        this.type = type;
        this.manager = manager;
        this.repository = repository;
        this.scripts = scripts;
        this.tags = tags;
        this.variables = variables;
        this.minimumRequirements = minimumRequirements;
        this.internal = internal;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Workflow{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", manager=").append(manager);
        sb.append(", tags=").append(tags);
        sb.append(", variables=").append(variables);
        sb.append(", minimumRequirements=").append(minimumRequirements);
        sb.append(", draft=").append(draft);
        sb.append(", repository=").append(repository);
        sb.append(", scripts=").append(scripts);
        sb.append(", version=").append(version);
        sb.append(", release=").append(release);
        sb.append(", internal=").append(internal);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    @Override
    public Workflow setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public Workflow setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Workflow setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getName() {
        return name;
    }

    public Workflow setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Workflow setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public Workflow setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Workflow setVersion(int version) {
        this.version = version;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Workflow setRelease(int release) {
        this.release = release;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Workflow setType(Type type) {
        this.type = type;
        return this;
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public Workflow setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public Workflow setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public Workflow setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Workflow setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<WorkflowVariable> getVariables() {
        return variables;
    }

    public Workflow setVariables(List<WorkflowVariable> variables) {
        this.variables = variables;
        return this;
    }

    public WorkflowMinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public Workflow setMinimumRequirements(WorkflowMinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public WorkflowInternal getInternal() {
        return internal;
    }

    public Workflow setInternal(WorkflowInternal internal) {
        this.internal = internal;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Workflow setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Workflow setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Workflow setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}