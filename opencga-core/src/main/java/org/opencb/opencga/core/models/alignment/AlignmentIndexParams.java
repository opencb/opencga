package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment index params";

    private String file;
    private boolean overwrite;

    public AlignmentIndexParams() {
    }

    public AlignmentIndexParams(String file, boolean overwrite) {
        this.file = file;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentIndexParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public AlignmentIndexParams setFile(String file) {
        this.file = file;
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
