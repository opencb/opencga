package org.opencb.opencga.core.config;

import java.util.ArrayList;
import java.util.List;

public class WorkflowConfiguration {

    private WorkflowMinRequirementsConfiguration minRequirements;
    private List<WorkflowSystemConfiguration> managers;

    public WorkflowConfiguration() {
        this.minRequirements = new WorkflowMinRequirementsConfiguration();
        this.managers = new ArrayList<>();
    }

    public WorkflowConfiguration(WorkflowMinRequirementsConfiguration minRequirements, List<WorkflowSystemConfiguration> managers) {
        this.minRequirements = minRequirements;
        this.managers = managers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowConfiguration{");
        sb.append("minRequirements=").append(minRequirements);
        sb.append(", managers=").append(managers);
        sb.append('}');
        return sb.toString();
    }

    public WorkflowMinRequirementsConfiguration getMinRequirements() {
        return minRequirements;
    }

    public WorkflowConfiguration setMinRequirements(WorkflowMinRequirementsConfiguration minRequirements) {
        this.minRequirements = minRequirements;
        return this;
    }

    public List<WorkflowSystemConfiguration> getManagers() {
        return managers;
    }

    public WorkflowConfiguration setManagers(List<WorkflowSystemConfiguration> managers) {
        this.managers = managers;
        return this;
    }
}
