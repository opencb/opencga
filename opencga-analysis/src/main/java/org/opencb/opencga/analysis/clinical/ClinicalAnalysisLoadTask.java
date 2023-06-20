package org.opencb.opencga.analysis.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisLoadParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.panel.PanelImportParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = ClinicalAnalysisLoadTask.ID, resource = Enums.Resource.DISEASE_PANEL, description = ClinicalAnalysisLoadTask.DESCRIPTION)
public class ClinicalAnalysisLoadTask extends OpenCgaToolScopeStudy {
    public final static String ID = "load";
    public static final String DESCRIPTION = "Load clinical analyses from a file";

    private String studyFqn;

    @ToolParams
    protected ClinicalAnalysisLoadParams params = new ClinicalAnalysisLoadParams();

    @Override
    protected void check() throws Exception {
        super.check();
        studyFqn = getStudy();

        if (StringUtils.isEmpty(params.getPath().toString())) {
            throw new ToolException("Missing input file when loading clinical analyses.");
        }

        if (!params.getPath().toFile().exists()) {
            throw new ToolException("Input file '" + params.getPath() + "' does not exist.");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> catalogManager.getClinicalAnalysisManager().load(studyFqn, params.getPath(), token));
    }

}
