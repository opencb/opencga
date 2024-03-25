package org.opencb.opencga.core.models.alignment;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class AlignmentIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment index params";

    @DataField(id = "fileId", description = FieldConstants.ALIGNMENT_INDEX_FILE_ID_DESCRIPTION, required = true)
    private String fileId;

    @DataField(id = "overwrite", description = FieldConstants.ALIGNMENT_INDEX_OVERWRITE_DESCRIPTION)
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
