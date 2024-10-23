package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class WorkflowMinimumRequirements {

    @DataField(id = "cpu", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_CPU_DESCRIPTION)
    private Integer cpu;

    @DataField(id = "memory", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_MEMORY_DESCRIPTION)
    private Integer memory;

    @DataField(id = "disk", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_DISK_DESCRIPTION)
    private Integer disk;

    public WorkflowMinimumRequirements() {
    }

    public WorkflowMinimumRequirements(Integer cpu, Integer memory, Integer disk) {
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

    public Integer getCpu() {
        return cpu;
    }

    public WorkflowMinimumRequirements setCpu(Integer cpu) {
        this.cpu = cpu;
        return this;
    }

    public Integer getMemory() {
        return memory;
    }

    public WorkflowMinimumRequirements setMemory(Integer memory) {
        this.memory = memory;
        return this;
    }

    public Integer getDisk() {
        return disk;
    }

    public WorkflowMinimumRequirements setDisk(Integer disk) {
        this.disk = disk;
        return this;
    }
}
