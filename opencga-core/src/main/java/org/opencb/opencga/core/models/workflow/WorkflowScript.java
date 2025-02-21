package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;

public class WorkflowScript {

    @DataField(id = "fileName", description = "File name")
    private String fileName;

    @DataField(id = "content", description = "Script content")
    private String content;

    @DataField(id = "main", description = "Flag indicating if this script is the main one.")
    private boolean main;

    public WorkflowScript() {
    }

    public WorkflowScript(String fileName, String content, boolean main) {
        this.fileName = fileName;
        this.content = content;
        this.main = main;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowScript{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", content='").append(content).append('\'');
        sb.append(", main=").append(main);
        sb.append('}');
        return sb.toString();
    }

    public String getFileName() {
        return fileName;
    }

    public WorkflowScript setFileName(String fileName) {
        this.fileName = fileName;
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
