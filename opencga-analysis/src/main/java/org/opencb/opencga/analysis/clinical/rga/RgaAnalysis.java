package org.opencb.opencga.analysis.clinical.rga;

import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.analysis.rga.RgaManager;
import org.opencb.opencga.core.models.clinical.RgaAnalysisParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = RgaAnalysis.ID, resource = Enums.Resource.RGA, type = Tool.Type.OPERATION, description = "Index RGA study.",
        priority = Enums.Priority.HIGH)
public class RgaAnalysis extends OperationTool {
    public final static String ID = "rga-index";
    public final static String DESCRIPTION = "Generate Recessive Gene Analysis secondary index";

    private String study;
    private String file;

    private RgaManager rgaManager;

    @Override
    protected void check() throws Exception {
        super.check();

        this.rgaManager = new RgaManager(getCatalogManager(), getVariantStorageManager());
        study = getStudyFqn();
        file = params.getString(RgaAnalysisParams.FILE);
    }

    @Override
    protected void run() throws Exception {
        step(() -> this.rgaManager.index(study, file, token));
    }

}
