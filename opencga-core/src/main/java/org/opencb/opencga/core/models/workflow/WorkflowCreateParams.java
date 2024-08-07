package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;
import java.util.Map;

public class WorkflowCreateParams {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.WORKFLOW_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

//    @DataField(id = "type", description = FieldConstants.WORKFLOW_TYPE_DESCRIPTION)
//    private Workflow.Type type;

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

    public WorkflowCreateParams() {
    }

    public WorkflowCreateParams(String id, String description, String commandLine, List<Workflow.Script> scripts,
                                String creationDate, String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.description = description;
        this.commandLine = commandLine;
        this.scripts = scripts;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", commandLine='").append(commandLine).append('\'');
        sb.append(", scripts=").append(scripts);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Workflow toWorkflow() {
        return new Workflow(id, description, 0, Workflow.Type.NEXTFLOW, commandLine, scripts, creationDate, modificationDate, attributes);
    }

    public String getId() {
        return id;
    }

    public WorkflowCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public WorkflowCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getCommandLine() {
        return commandLine;
    }

    public WorkflowCreateParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public List<Workflow.Script> getScripts() {
        return scripts;
    }

    public WorkflowCreateParams setScripts(List<Workflow.Script> scripts) {
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
