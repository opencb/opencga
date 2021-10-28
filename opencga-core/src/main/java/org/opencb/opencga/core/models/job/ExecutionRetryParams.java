package org.opencb.opencga.core.models.job;

public class ExecutionRetryParams {

    private String execution;

    public ExecutionRetryParams() {
    }

    public ExecutionRetryParams(String execution) {
        this.execution = execution;
    }

    public String getExecution() {
        return execution;
    }

    public ExecutionRetryParams setExecution(String execution) {
        this.execution = execution;
        return this;
    }
}
