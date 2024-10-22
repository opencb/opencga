package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class WorkflowMinimumRequirements {

    @DataField(id = "cpu", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_CPU_DESCRIPTION)
    private String cpu;

    @DataField(id = "memory", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_MEMORY_DESCRIPTION)
    private String memory;

    @DataField(id = "disk", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_DISK_DESCRIPTION)
    private String disk;

    public WorkflowMinimumRequirements() {
    }

    public WorkflowMinimumRequirements(String cpu, String memory, String disk) {
        this.cpu = cpu;
        this.memory = memory;
        this.disk = disk;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowMinimumRequirements{");
        sb.append("cpu='").append(cpu).append('\'');
        sb.append(", memory='").append(memory).append('\'');
        sb.append(", disk='").append(disk).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCpu() {
        return cpu;
    }

    public WorkflowMinimumRequirements setCpu(String cpu) {
        this.cpu = cpu;
        return this;
    }

    public String getMemory() {
        return memory;
    }

    public WorkflowMinimumRequirements setMemory(String memory) {
        this.memory = memory;
        return this;
    }

    public String getDisk() {
        return disk;
    }

    public WorkflowMinimumRequirements setDisk(String disk) {
        this.disk = disk;
        return this;
    }
}
