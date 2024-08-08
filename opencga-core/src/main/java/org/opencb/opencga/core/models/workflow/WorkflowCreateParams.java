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

//    @DataField(id = "type", description = FieldConstants.WORKFLOW_TYPE_DESCRIPTION)
//    private Workflow.Type type;

    @DataField(id = "docker", description = FieldConstants.WORKFLOW_DOCKER_DESCRIPTION)
    private WorkflowRepository docker;

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

    public WorkflowCreateParams(String id, String name, String description, WorkflowRepository docker, List<WorkflowScript> scripts,
                                String creationDate, String modificationDate, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.docker = docker;
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
        sb.append(", docker=").append(docker);
        sb.append(", scripts=").append(scripts);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public Workflow toWorkflow() {
        return new Workflow(id, name, description, Workflow.Type.NEXTFLOW, docker, scripts, creationDate, modificationDate, attributes);
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

    public WorkflowRepository getDocker() {
        return docker;
    }

    public WorkflowCreateParams setDocker(WorkflowRepository docker) {
        this.docker = docker;
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
