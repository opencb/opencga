package org.opencb.opencga.core.models.alignment;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class CoverageIndexParams extends ToolParams {

    @DataField(id = "fileId", description = FieldConstants.ALIGNMENT_BAM_FILE_ID_DESCRIPTION)
    private String fileId;

    @JsonProperty(defaultValue = "50")
    @DataField(id = "windowSize", defaultValue = "50", description = FieldConstants.ALIGNMENT_WINDOW_SIZE_DESCRIPTION)
    private int windowSize;

    @DataField(id = "overwrite", description = FieldConstants.ALIGNMENT_OVERWRITE_DESCRIPTION)
    private boolean overwrite;

    public CoverageIndexParams() {
    }

    public CoverageIndexParams(String fileId, int windowSize, boolean overwrite) {
        this.fileId = fileId;
        this.windowSize = windowSize;
        this.overwrite = overwrite;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CoverageIndexParams{");
        sb.append("fileId='").append(fileId).append('\'');
        sb.append(", windowSize=").append(windowSize);
        sb.append(", overwrite=").append(overwrite);
        sb.append('}');
        return sb.toString();
    }

    public String getFileId() {
        return fileId;
    }

    public CoverageIndexParams setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public CoverageIndexParams setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public CoverageIndexParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }
}
