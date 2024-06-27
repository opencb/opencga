package org.opencb.opencga.core.config;

public class OperationExecutionConfig {

    private Policy policy;
    private int maxRetryAttempts;

    public enum Policy {
        IMMEDIATE,
        NIGHTLY,
        NEVER
    }

    public OperationExecutionConfig() {
    }

    public OperationExecutionConfig(Policy policy, int maxRetryAttempts) {
        this.policy = policy;
        this.maxRetryAttempts = maxRetryAttempts;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OperationExecutionConfig{");
        sb.append("policy=").append(policy);
        sb.append(", maxRetryAttempts=").append(maxRetryAttempts);
        sb.append('}');
        return sb.toString();
    }

    public Policy getPolicy() {
        return policy;
    }

    public OperationExecutionConfig setPolicy(Policy policy) {
        this.policy = policy;
        return this;
    }

    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    public OperationExecutionConfig setMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
        return this;
    }
}
