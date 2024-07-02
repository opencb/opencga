package org.opencb.opencga.core.config;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import java.util.Map;

public class OperationExecutionConfig {

    @DataField(id = "policy", description = FieldConstants.OPERATION_EXECUTION_CONFIG_POLICY)
    private Policy policy;

    @DataField(id = "maxAttempts", description = FieldConstants.OPERATION_EXECUTION_CONFIG_MAX_ATTEMPTS)
    private int maxAttempts;

    @DataField(id = "jobParams", description = FieldConstants.OPERATION_EXECUTION_CONFIG_JOB_PARAMS)
    private Map<String, String> jobParams;

    public enum Policy {
        IMMEDIATE,
        NIGHTLY,
        NEVER
    }

    public OperationExecutionConfig() {
    }

    public OperationExecutionConfig(Policy policy, int maxAttempts) {
        this.policy = policy;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OperationExecutionConfig{");
        sb.append("policy=").append(policy);
        sb.append(", maxAttempts=").append(maxAttempts);
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

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public OperationExecutionConfig setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
    }

    public OperationExecutionConfig setJobParams(Map<String, String> jobParams) {
        this.jobParams = jobParams;
        return this;
    }

    public Map<String, String> getJobParams() {
        return jobParams;
    }
}
