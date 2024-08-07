package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;
import java.util.Map;

public class WorkflowUpdateParams {

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "commandLine", description = FieldConstants.WORKFLOW_COMMAND_LINE_DESCRIPTION)
    private String commandLine;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<Workflow.Script> scripts;

    @DataField(id = "creationDate", indexed = true, description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "attributes", description = FieldConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public WorkflowUpdateParams() {
    }

    public WorkflowUpdateParams(String description, String commandLine, List<Workflow.Script> scripts,
                                String creationDate, String modificationDate, Map<String, Object> attributes) {
        this.description = description;
        this.commandLine = commandLine;
        this.scripts = scripts;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowUpdateParams{");
        sb.append("description='").append(description).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", scripts=").append(scripts);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public WorkflowUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public WorkflowUpdateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public List<Workflow.Script> getScripts() {
        return scripts;
    }

    public WorkflowUpdateParams setScripts(List<Workflow.Script> scripts) {
        this.scripts = scripts;
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
