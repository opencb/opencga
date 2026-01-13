package org.opencb.opencga.core.models.externalTool.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.List;

public class Workflow {

    @DataField(id = "manager", description = FieldConstants.WORKFLOW_MANAGER_DESCRIPTION)
    private WorkflowSystem manager;

    @DataField(id = "scripts", description = FieldConstants.WORKFLOW_SCRIPTS_DESCRIPTION)
    private List<WorkflowScript> scripts;

    @DataField(id = "repository", description = FieldConstants.WORKFLOW_REPOSITORY_DESCRIPTION)
    private WorkflowRepository repository;

    public Workflow() {
    }

    public Workflow(WorkflowSystem manager, List<WorkflowScript> scripts, WorkflowRepository repository) {
        this.manager = manager;
        this.scripts = scripts;
        this.repository = repository;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Workflow{");
        sb.append("manager=").append(manager);
        sb.append(", scripts=").append(scripts);
        sb.append(", repository=").append(repository);
        sb.append('}');
        return sb.toString();
    }

    public WorkflowSystem getManager() {
        return manager;
    }

    public Workflow setManager(WorkflowSystem manager) {
        this.manager = manager;
        return this;
    }

    public List<WorkflowScript> getScripts() {
        return scripts;
    }

    public Workflow setScripts(List<WorkflowScript> scripts) {
        this.scripts = scripts;
        return this;
    }

    public WorkflowRepository getRepository() {
        return repository;
    }

    public Workflow setRepository(WorkflowRepository repository) {
        this.repository = repository;
        return this;
    }

}
