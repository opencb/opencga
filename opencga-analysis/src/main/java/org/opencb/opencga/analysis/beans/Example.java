package org.opencb.opencga.analysis.beans;

import java.util.List;

public class Example {
    private String name, executionId;
    private List<ExampleOption> options;

    public Example() {

    }

    public Example(String name, String executionId, List<ExampleOption> options) {
        this.name = name;
        this.executionId = executionId;
        this.options = options;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public List<ExampleOption> geOptions() {
        return options;
    }

    public void setOptions(List<ExampleOption> options) {
        this.options = options;
    }
}
