package org.opencb.opencga.core.models.externalTool;

import org.opencb.commons.annotations.DataField;

public class WorkflowScript {

    @DataField(id = "name", description = "File name")
    private String name;

    @DataField(id = "content", description = "Script content")
    private String content;

    @DataField(id = "main", description = "Flag indicating if this script is the main one.")
    private boolean main;

    public WorkflowScript() {
    }

    public WorkflowScript(String name, String content, boolean main) {
        this.name = name;
        this.content = content;
        this.main = main;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowScript{");
        sb.append("name='").append(name).append('\'');
        sb.append(", content='").append(content).append('\'');
        sb.append(", main=").append(main);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public WorkflowScript setName(String name) {
        this.name = name;
        return this;
    }

    public String getContent() {
        return content;
    }

    public WorkflowScript setContent(String content) {
        this.content = content;
        return this;
    }

    public boolean isMain() {
        return main;
    }

    public WorkflowScript setMain(boolean main) {
        this.main = main;
        return this;
    }
}
