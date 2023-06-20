package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.nio.file.Path;

public class ClinicalAnalysisLoadParams extends ToolParams {

    @DataField(id = ParamConstants.FILE_PATH_PARAM, description = ParamConstants.FILE_PATH_DESCRIPTION)
    private Path path;

    public ClinicalAnalysisLoadParams() {
    }

    public ClinicalAnalysisLoadParams(Path path) {
        this.path = path;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisLoadParams{");
        sb.append("path=").append(path);
        sb.append('}');
        return sb.toString();
    }

    public Path getPath() {
        return path;
    }

    public ClinicalAnalysisLoadParams setPath(Path path) {
        this.path = path;
        return this;
    }
}
