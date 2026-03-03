package org.opencb.opencga.core.models.file;

public class FileContentUpdateParams {

    private String content;

    public FileContentUpdateParams() {
    }

    public FileContentUpdateParams(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileContentUpdateParams{");
        sb.append("content='").append(content).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getContent() {
        return content;
    }

    public FileContentUpdateParams setContent(String content) {
        this.content = content;
        return this;
    }
}
