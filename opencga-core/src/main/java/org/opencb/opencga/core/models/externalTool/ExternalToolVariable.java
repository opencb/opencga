package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;

public class ExternalToolVariable {

    @DataField(id = "id", description = "Variable identifier (e.g., 'inputFile', '--threads', '-q').")
    private String id;

    @DataField(id = "name", description = "Pretty name of the variable.")
    private String name;

    @DataField(id = "description", description = "Variable description.")
    private String description;

    @DataField(id = "type", description = "Variable type. May be: FLAG, BOOLEAN, INTEGER, DOUBLE, STRING, FILE.")
    private ExternalToolVariableType type;

    @DataField(id = "required", description = "Whether the variable is required or not.")
    private boolean required;

    @DataField(id = "defaultValue", description = "Default value for the variable (if any).")
    private String defaultValue;

    @DataField(id = "output", description = "Whether the variable is an output file or directory.")
    private boolean output;

    public enum ExternalToolVariableType {
        FLAG,
        BOOLEAN,
        INTEGER,
        DOUBLE,
        STRING,
        FILE
    }

    public ExternalToolVariable() {
    }

    public ExternalToolVariable(String id, String name, String description, ExternalToolVariableType type, boolean required,
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

    public ExternalToolVariableType getType() {
        return type;
    }

    public ExternalToolVariable setType(ExternalToolVariableType type) {
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
