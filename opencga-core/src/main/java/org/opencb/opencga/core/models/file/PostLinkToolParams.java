package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class PostLinkToolParams extends ToolParams {

    public static final String DESCRIPTION = "File postlink params";

    private List<String> files;

    public PostLinkToolParams() {
    }

    public PostLinkToolParams(List<String> files) {
        this.files = files;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PostLinkToolParams{");
        sb.append("files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getFiles() {
        return files;
    }

    public PostLinkToolParams setFiles(List<String> files) {
        this.files = files;
        return this;
    }
}
