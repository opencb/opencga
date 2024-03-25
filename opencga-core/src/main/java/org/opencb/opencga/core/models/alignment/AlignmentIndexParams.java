package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment index params";

    private String fileId;
    private boolean overwrite;

    public AlignmentIndexParams() {
    }

    public AlignmentIndexParams(String fileId, boolean overwrite) {
        this.fileId = fileId;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentIndexParams{");
        sb.append("fileId='").append(fileId).append('\'');
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public String getFileId() {
        return fileId;
    }

    public AlignmentIndexParams setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public AlignmentIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
