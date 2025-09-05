package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.job.MinimumRequirements;

import java.util.List;
import java.util.Map;

public abstract class ExternalToolUpdateParams {

    @DataField(id = "name", description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String name;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "scope", description = FieldConstants.EXTERNAL_TOOL_SCOPE_DESCRIPTION)
    private ExternalToolScope scope;

    @DataField(id = "tags", description = FieldConstants.EXTERNAL_TOOL_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "variables", description = FieldConstants.EXTERNAL_TOOL_VARIABLES_DESCRIPTION)
    private List<ExternalToolVariable> variables;

    @DataField(id = "minimumRequirements", description = FieldConstants.MINIMUM_REQUIREMENTS_DESCRIPTION)
    private MinimumRequirements minimumRequirements;

    @DataField(id = "draft", description = FieldConstants.EXTERNAL_TOOL_DRAFT_DESCRIPTION)
    private boolean draft;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public ExternalToolUpdateParams() {
    }

    public ExternalToolUpdateParams(String name, String description, ExternalToolScope scope, List<String> tags,
                                    List<ExternalToolVariable> variables, MinimumRequirements minimumRequirements, boolean draft,
                                    String creationDate, String modificationDate, Map<String, Object> attributes) {
        this.name = name;
        this.description = description;
        this.scope = scope;
        this.tags = tags;
        this.variables = variables;
        this.minimumRequirements = minimumRequirements;
        this.draft = draft;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", scope=").append(scope);
        sb.append(", tags=").append(tags);
        sb.append(", variables=").append(variables);
        sb.append(", minimumRequirements=").append(minimumRequirements);
        sb.append(", draft=").append(draft);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes).append('\'');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public ExternalToolUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExternalToolUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ExternalToolScope getScope() {
        return scope;
    }

    public ExternalToolUpdateParams setScope(ExternalToolScope scope) {
        this.scope = scope;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public ExternalToolUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public List<ExternalToolVariable> getVariables() {
        return variables;
    }

    public ExternalToolUpdateParams setVariables(List<ExternalToolVariable> variables) {
        this.variables = variables;
        return this;
    }

    public MinimumRequirements getMinimumRequirements() {
        return minimumRequirements;
    }

    public ExternalToolUpdateParams setMinimumRequirements(MinimumRequirements minimumRequirements) {
        this.minimumRequirements = minimumRequirements;
        return this;
    }

    public boolean isDraft() {
        return draft;
    }

    public ExternalToolUpdateParams setDraft(boolean draft) {
        this.draft = draft;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public ExternalToolUpdateParams setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public ExternalToolUpdateParams setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ExternalToolUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
