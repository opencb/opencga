package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class AlignmentIndexParams extends ToolParams {
    public static final String DESCRIPTION = "Alignment index params";

    @DataField(description = ParamConstants.ALIGNMENT_INDEX_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.ALIGNMENT_INDEX_PARAMS_OVERWRITE_DESCRIPTION)
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
