package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class PostLinkToolParams extends ToolParams {

    public static final String DESCRIPTION = "File postlink params";

    private List<String> files;
    private Integer batchSize;

    public PostLinkToolParams() {
    }

    public PostLinkToolParams(List<String> files, Integer batchSize) {
        this.files = files;
        this.batchSize = batchSize;
    }

    public List<String> getFiles() {
        return files;
    }

    public PostLinkToolParams setFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public PostLinkToolParams setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }
}
