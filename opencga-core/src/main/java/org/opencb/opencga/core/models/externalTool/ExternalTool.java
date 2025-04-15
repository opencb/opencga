package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

@DataClass(id = "ExternalTool", since = "3.3.0", description = "External Tool.")
public class ExternalTool extends PrivateStudyUid {

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

    @DataField(id = "scope", description = FieldConstants.WORKFLOW_SCOPE_DESCRIPTION)
    private Scope scope;

    @DataField(id = "manager", description = FieldConstants.WORKFLOW_MANAGER_DESCRIPTION)
    private WorkflowSystem manager;

    @DataField(id = "tags", description = FieldConstants.WORKFLOW_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "variables", description = FieldConstants.WORKFLOW_VARIABLES_DESCRIPTION)
    private List<ExternalToolVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.MINIMUM_REQUIREMENTS_DESCRIPTION)
    private MinimumRequirements minimumRequirements;

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
    private ExternalToolInternal internal;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public enum Type {
        TOOL,
        VARIANT_WALKER,
        WORKFLOW
    }

    public enum Scope {
        SECONDARY_ANALYSIS,
        RESEARCH_ANALYSIS,
        CLINICAL_INTERPRETATION_ANALYSIS,
        OTHER
    }

    public ExternalTool() {
    }

    public ExternalTool(String id, String name, String description, Scope scope, WorkflowSystem manager, List<String> tags,
                        List<ExternalToolVariable> variables, MinimumRequirements minimumRequirements, boolean draft,
                        WorkflowRepository repository, List<WorkflowScript> scripts, ExternalToolInternal internal, String creationDate,
                        String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.draft = draft;
        this.scope = scope;
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
        sb.append(", scope=").append(scope);
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
    public ExternalTool setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public ExternalTool setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public ExternalTool setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public String getName() {
        return name;
    }

    public ExternalTool setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExternalTool setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public ExternalTool setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public ExternalTool setVersion(int version) {
        this.version = version;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ExternalTool setRelease(int release) {
        this.release = release;
        return this;
    }

    public Scope getScope() {
        return scope;
    }

    public ExternalTool setScope(Scope scope) {
        this.scope = scope;
        return this;
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public ExternalTool setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public ExternalTool setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public ExternalTool setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public ExternalTool setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<ExternalToolVariable> getVariables() {
        return variables;
    }

    public ExternalTool setVariables(List<ExternalToolVariable> variables) {
        this.variables = variables;
        return this;
    }

    public MinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public ExternalTool setMinimumRequirements(MinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public ExternalToolInternal getInternal() {
        return internal;
    }

    public ExternalTool setInternal(ExternalToolInternal internal) {
        this.internal = internal;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ExternalTool setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ExternalTool setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ExternalTool setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
