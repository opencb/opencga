package org.opencb.opencga.core.config;

import org.opencb.commons.annotations.DataField;

public class ExecutionFactor {

    @DataField(id = "cpu", description = "CPU factor to be applied to the cpu requested by the job. Must be a value between 0 and 1.")
    public float cpu;

    @DataField(id = "memory", description = "Memory factor to be applied to the memory requested by the job."
            + " Must be a value between 0 and 1.")
    public float memory;

    public ExecutionFactor() {
    }

    public ExecutionFactor(float cpu, float memory) {
        this.cpu = cpu;
        this.memory = memory;
    }

    public static ExecutionFactor defaultFactor() {
        return new ExecutionFactor(0.95f, 0.9f);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionFactor{");
        sb.append("cpu=").append(cpu);
        sb.append(", memory=").append(memory);
        sb.append('}');
        return sb.toString();
    }

    public float getCpu() {
        return cpu;
    }

    public ExecutionFactor setCpu(float cpu) {
        this.cpu = cpu;
        return this;
    }

    public float getMemory() {
        return memory;
    }

    public ExecutionFactor setMemory(float memory) {
        this.memory = memory;
        return this;
    }
}
