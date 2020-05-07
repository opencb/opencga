package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment index params";

    private String file;

    public AlignmentIndexParams() {
    }

    public AlignmentIndexParams(String file) {
        this.file = file;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentIndexParams{");
        sb.append("file='").append(file).append('\'');
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
}
