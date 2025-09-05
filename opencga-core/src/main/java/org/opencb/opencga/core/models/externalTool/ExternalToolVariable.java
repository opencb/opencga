package org.opencb.opencga.core.models.externalTool;

public class ExternalToolVariable {

    private String id;
    private String name;
    private String description;
    private WorkflowVariableType type;
    private boolean required;
    private String defaultValue;
    private boolean output;

    public enum WorkflowVariableType {
        FLAG,
        BOOLEAN,
        INTEGER,
        DOUBLE,
        STRING,
        FILE
    }

    public ExternalToolVariable() {
    }

    public ExternalToolVariable(String id, String name, String description, WorkflowVariableType type, boolean required,
                                String defaultValue, boolean output) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
        this.required = required;
        this.defaultValue = defaultValue;
        this.output = output;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowVariable{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", required=").append(required);
        sb.append(", defaultValue='").append(defaultValue).append('\'');
        sb.append(", output=").append(output);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ExternalToolVariable setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ExternalToolVariable setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExternalToolVariable setDescription(String description) {
        this.description = description;
        return this;
    }

    public WorkflowVariableType getType() {
        return type;
    }

    public ExternalToolVariable setType(WorkflowVariableType type) {
        this.type = type;
        return this;
    }

    public boolean isRequired() {
        return required;
    }

    public ExternalToolVariable setRequired(boolean required) {
        this.required = required;
        return this;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public ExternalToolVariable setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public boolean isOutput() {
        return output;
    }

    public ExternalToolVariable setOutput(boolean output) {
        this.output = output;
        return this;
    }
}
