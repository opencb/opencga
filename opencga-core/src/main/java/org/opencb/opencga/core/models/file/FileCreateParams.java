package org.opencb.opencga.core.models.file;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FileCreateParams {

    @JsonProperty(required = true)
    private String path;
    private String content;
    private String description;

    @JsonProperty(defaultValue = "false")
    private boolean parents;

    @JsonProperty(defaultValue = "false")
    private boolean directory;

    public FileCreateParams() {
    }

    public FileCreateParams(String path, String content, String description, boolean parents, boolean directory) {
        this.path = path;
        this.content = content;
        this.description = description;
        this.parents = parents;
        this.directory = directory;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileCreateParams{");
        sb.append("path='").append(path).append('\'');
        sb.append(", content='").append(content).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", parents=").append(parents);
        sb.append(", directory=").append(directory);
        sb.append('}');
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public FileCreateParams setPath(String path) {
        this.path = path;
        return this;
    }

    public String getContent() {
        return content;
    }

    public FileCreateParams setContent(String content) {
        this.content = content;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileCreateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isParents() {
        return parents;
    }

    public FileCreateParams setParents(boolean parents) {
        this.parents = parents;
        return this;
    }

    public boolean isDirectory() {
        return directory;
    }

    public FileCreateParams setDirectory(boolean directory) {
        this.directory = directory;
        return this;
    }
}
