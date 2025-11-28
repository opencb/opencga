package org.opencb.opencga.core.config;

import org.opencb.commons.annotations.DataField;
import org.opencb.commons.datastore.core.ObjectMap;

public class ExecutionQueue {

    @DataField(id = "id", description = "Queue identifier")
    private String id;

    @DataField(id = "executor", description = "Executor identifier")
    private String executor;

    @DataField(id = "description", description = "Description of the queue")
    private String description;

    @DataField(id = "processorType", description = "Execution type for the queue")
    private ProcessorType processorType;

    @DataField(id = "cpu", description = "Number of CPUs allocated for this queue")
    private String cpu;

    @DataField(id = "memory", description = "Memory allocated for this queue")
    private String memory;

    @DataField(id = "options", description = "Additional options for the queue")
    private ObjectMap options;

    public enum ProcessorType {
        CPU, // Default execution type
        GPU, // Execution type for GPU-based tasks
        FPGA // Execution type for FPGA-based tasks
    }

    public ExecutionQueue() {
    }

    public ExecutionQueue(String id, String executor, String description, ProcessorType processorType, String cpu, String memory,
                          ObjectMap options) {
        this.id = id;
        this.executor = executor;
        this.description = description;
        this.processorType = processorType;
        this.cpu = cpu;
        this.memory = memory;
        this.options = options;
    }

    public static ExecutionQueue defaultQueue() {
        return new ExecutionQueue("default", "local", "Default local executor", ExecutionQueue.ProcessorType.CPU, "8", "16G",
                new ObjectMap());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionQueue{");
        sb.append("id='").append(id).append('\'');
        sb.append(", executor='").append(executor).append('\'');
        sb.append(", processorType=").append(processorType);
        sb.append(", cpu=").append(cpu);
        sb.append(", memory='").append(memory).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExecutionQueue setId(String id) {
        this.id = id;
        return this;
    }

    public String getExecutor() {
        return executor;
    }

    public ExecutionQueue setExecutor(String executor) {
        this.executor = executor;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExecutionQueue setDescription(String description) {
        this.description = description;
        return this;
    }

    public ProcessorType getProcessorType() {
        return processorType;
    }

    public ExecutionQueue setProcessorType(ProcessorType type) {
        this.processorType = type;
        return this;
    }

    public String getCpu() {
        return cpu;
    }

    public ExecutionQueue setCpu(String cpu) {
        this.cpu = cpu;
        return this;
    }

    public String getMemory() {
        return memory;
    }

    public ExecutionQueue setMemory(String memory) {
        this.memory = memory;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public ExecutionQueue setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
