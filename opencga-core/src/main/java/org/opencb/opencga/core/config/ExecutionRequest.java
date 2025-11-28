package org.opencb.opencga.core.config;

import org.opencb.commons.annotations.DataField;

public class ExecutionRequest {

    @DataField(id = "cpu", description = "Default cpu for new execution requests..")
    public int cpu;

    @DataField(id = "memory", description = "Default memory for new execution requests.")
    public String memory;

    public ExecutionRequest() {
    }

    public ExecutionRequest(int cpu, String memory) {
        this.cpu = cpu;
        this.memory = memory;
    }

    public static ExecutionRequest defaultRequest() {
        return new ExecutionRequest(2, "8G");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionRequest{");
        sb.append("cpu=").append(cpu);
        sb.append(", memory='").append(memory).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getCpu() {
        return cpu;
    }

    public ExecutionRequest setCpu(int cpu) {
        this.cpu = cpu;
        return this;
    }

    public String getMemory() {
        return memory;
    }

    public ExecutionRequest setMemory(String memory) {
        this.memory = memory;
        return this;
    }
}
