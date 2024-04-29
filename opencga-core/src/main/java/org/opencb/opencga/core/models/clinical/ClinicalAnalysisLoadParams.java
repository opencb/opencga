package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.nio.file.Path;

public class ClinicalAnalysisLoadParams extends ToolParams {
    public static final String DESCRIPTION = "Parameters to load clinical analysis in OpenCGA catalog from a file";
    public static final String FILE = "file";

    private String file;

    public ClinicalAnalysisLoadParams() {
    }

    public ClinicalAnalysisLoadParams(String file) {
        this.file = file;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisLoadParams{");
        sb.append("file=").append(file);
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public ClinicalAnalysisLoadParams setFile(String file) {
        this.file = file;
        return this;
    }
}
