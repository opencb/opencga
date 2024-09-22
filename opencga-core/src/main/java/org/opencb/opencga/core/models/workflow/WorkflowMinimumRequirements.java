package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class WorkflowMinimumRequirements {

    @DataField(id = "cpu", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_CPU_DESCRIPTION)
    private Integer cpu;

    @DataField(id = "memory", description = FieldConstants.WORKFLOW_MIN_REQUIREMENTS_MEMORY_DESCRIPTION)
    private Integer memory;

    public WorkflowMinimumRequirements() {
    }

    public WorkflowMinimumRequirements(int cpu, int memory) {
        this.cpu = cpu;
        this.memory = memory;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowMinimumRequirements{");
        sb.append("cpu=").append(cpu);
        sb.append(", memory=").append(memory);
        sb.append('}');
        return sb.toString();
    }

    public int getCpu() {
        return cpu;
    }

    public WorkflowMinimumRequirements setCpu(int cpu) {
        this.cpu = cpu;
        return this;
    }

    public int getMemory() {
        return memory;
    }

    public WorkflowMinimumRequirements setMemory(int memory) {
        this.memory = memory;
        return this;
    }
}
