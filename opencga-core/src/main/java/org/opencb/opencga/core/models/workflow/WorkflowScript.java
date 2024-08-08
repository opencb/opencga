package org.opencb.opencga.core.models.workflow;

import org.opencb.commons.annotations.DataField;

public class WorkflowScript {

    @DataField(id = "filename", description = "File name")
    private String filename;

    @DataField(id = "content", description = "Script content")
    private String content;

    @DataField(id = "main", description = "Flag indicating if this script is the main one.")
    private boolean main;

    public WorkflowScript() {
    }

    public WorkflowScript(String filename, String content, boolean main) {
        this.filename = filename;
        this.content = content;
        this.main = main;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowScript{");
        sb.append("filename='").append(filename).append('\'');
        sb.append(", content='").append(content).append('\'');
        sb.append(", main=").append(main);
        sb.append('}');
        return sb.toString();
    }

    public String getFilename() {
        return filename;
    }

    public WorkflowScript setFilename(String filename) {
        this.filename = filename;
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
