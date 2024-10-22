package org.opencb.opencga.core.config;

public class WorkflowMinRequirementsConfiguration {

    private String cpu;
    private String memory;
    private String disk;

    public WorkflowMinRequirementsConfiguration() {
    }

    public WorkflowMinRequirementsConfiguration(String cpu, String memory, String disk) {
        this.cpu = cpu;
        this.memory = memory;
        this.disk = disk;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowMinRequirementsConfiguration{");
        sb.append("cpu='").append(cpu).append('\'');
        sb.append(", memory='").append(memory).append('\'');
        sb.append(", disk='").append(disk).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCpu() {
        return cpu;
    }

    public WorkflowMinRequirementsConfiguration setCpu(String cpu) {
        this.cpu = cpu;
        return this;
    }

    public String getMemory() {
        return memory;
    }

    public WorkflowMinRequirementsConfiguration setMemory(String memory) {
        this.memory = memory;
        return this;
    }

    public String getDisk() {
        return disk;
    }

    public WorkflowMinRequirementsConfiguration setDisk(String disk) {
        this.disk = disk;
        return this;
    }
}
