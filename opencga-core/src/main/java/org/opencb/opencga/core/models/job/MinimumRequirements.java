package org.opencb.opencga.core.models.job;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.config.ExecutionQueue;

public class MinimumRequirements {

    @DataField(id = "cpu", description = FieldConstants.MIN_REQUIREMENTS_CPU_DESCRIPTION)
    private String cpu;

    @DataField(id = "memory", description = FieldConstants.MIN_REQUIREMENTS_MEMORY_DESCRIPTION)
    private String memory;

    @DataField(id = "disk", description = FieldConstants.MIN_REQUIREMENTS_DISK_DESCRIPTION)
    private String disk;

    @DataField(id = "processorType", description = FieldConstants.MIN_REQUIREMENTS_TYPE_DESCRIPTION)
    private ExecutionQueue.ProcessorType processorType;

    @DataField(id = "queue", description = FieldConstants.MIN_REQUIREMENTS_QUEUE_DESCRIPTION)
    private String queue;

    public MinimumRequirements() {
    }

    public MinimumRequirements(String cpu, String memory, String disk, ExecutionQueue.ProcessorType processorType, String queue) {
        this.cpu = cpu;
        this.memory = memory;
        this.disk = disk;
        this.processorType = processorType;
        this.queue = queue;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MinimumRequirements{");
        sb.append("cpu='").append(cpu).append('\'');
        sb.append(", memory='").append(memory).append('\'');
        sb.append(", disk='").append(disk).append('\'');
        sb.append(", processorType=").append(processorType);
        sb.append(", queue='").append(queue).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCpu() {
        return cpu;
    }

    public MinimumRequirements setCpu(String cpu) {
        this.cpu = cpu;
        return this;
    }

    public String getMemory() {
        return memory;
    }

    public MinimumRequirements setMemory(String memory) {
        this.memory = memory;
        return this;
    }

    public String getDisk() {
        return disk;
    }

    public MinimumRequirements setDisk(String disk) {
        this.disk = disk;
        return this;
    }

    public ExecutionQueue.ProcessorType getProcessorType() {
        return processorType;
    }

    public MinimumRequirements setProcessorType(ExecutionQueue.ProcessorType processorType) {
        this.processorType = processorType;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public MinimumRequirements setQueue(String queue) {
        this.queue = queue;
        return this;
    }
}
