package org.opencb.opencga.core.models.externalTool.custom;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.externalTool.Docker;
import org.opencb.opencga.core.models.externalTool.ExternalToolInternal;
import org.opencb.opencga.core.models.externalTool.ExternalToolScope;
import org.opencb.opencga.core.models.externalTool.ExternalToolVariable;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

public class CustomToolCreateParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.EXTERNAL_TOOL_ID_DESCRIPTION)
    private String id;

    @DataField(id = "name", description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String name;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "scope", description = FieldConstants.EXTERNAL_TOOL_SCOPE_DESCRIPTION)
    private ExternalToolScope scope;

    @DataField(id = "docker", description = FieldConstants.EXTERNAL_TOOL_DOCKER_DESCRIPTION)
    private Docker docker;

    @DataField(id = "tags", description = FieldConstants.EXTERNAL_TOOL_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "variables", description = FieldConstants.EXTERNAL_TOOL_VARIABLES_DESCRIPTION)
    private List<ExternalToolVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.MINIMUM_REQUIREMENTS_DESCRIPTION)
    private MinimumRequirements minimumRequirements;

    @DataField(id = "draft", description = FieldConstants.EXTERNAL_TOOL_DRAFT_DESCRIPTION)
    private boolean draft;

    @DataField(id = "internal", description = FieldConstants.EXTERNAL_TOOL_INTERNAL_DESCRIPTION)
    private ExternalToolInternal internal;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public CustomToolCreateParams() {
    }

    public CustomToolCreateParams(String id, String name, String description, ExternalToolScope scope, Docker docker, List<String> tags,
                                  List<ExternalToolVariable> variables, MinimumRequirements minimumRequirements, boolean draft,
                                  ExternalToolInternal internal, String creationDate, String modificationDate,
                                  Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.scope = scope;
        this.docker = docker;
        this.tags = tags;
        this.variables = variables;
        this.minimumRequirements = minimumRequirements;
        this.draft = draft;
        this.internal = internal;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomToolCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", scope=").append(scope);
        sb.append(", docker=").append(docker);
        sb.append(", tags=").append(tags);
        sb.append(", variables=").append(variables);
        sb.append(", minimumRequirements=").append(minimumRequirements);
        sb.append(", draft=").append(draft);
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

    public CustomToolCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public CustomToolCreateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public CustomToolCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ExternalToolScope getScope() {
        return scope;
    }

    public CustomToolCreateParams setScope(ExternalToolScope scope) {
        this.scope = scope;
        return this;
    }

    public Docker getDocker() {
        return docker;
    }

    public CustomToolCreateParams setDocker(Docker docker) {
        this.docker = docker;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public CustomToolCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<ExternalToolVariable> getVariables() {
        return variables;
    }

    public CustomToolCreateParams setVariables(List<ExternalToolVariable> variables) {
        this.variables = variables;
        return this;
    }

    public MinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public CustomToolCreateParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public CustomToolCreateParams setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public ExternalToolInternal getInternal() {
        return internal;
    }

    public CustomToolCreateParams setInternal(ExternalToolInternal internal) {
        this.internal = internal;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public CustomToolCreateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public CustomToolCreateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public CustomToolCreateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
