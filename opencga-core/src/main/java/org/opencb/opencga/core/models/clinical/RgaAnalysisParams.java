package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class RgaAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Recessive Gene Analysis index params";
    public static final String FILE = "file";

    @DataField(description = ParamConstants.RGA_ANALYSIS_PARAMS_FILE_DESCRIPTION)
    private String file;

    public RgaAnalysisParams() {
    }

    public RgaAnalysisParams(String file) {
        this.file = file;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RgaAnalysisParams{");
        sb.append(", file='").append(file).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public RgaAnalysisParams setFile(String file) {
        this.file = file;
        return this;
    }
}
