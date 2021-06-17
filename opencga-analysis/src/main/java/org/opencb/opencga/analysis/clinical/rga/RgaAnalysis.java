package org.opencb.opencga.analysis.clinical.rga;

import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.analysis.rga.RgaManager;
import org.opencb.opencga.core.models.clinical.RgaAnalysisParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = RgaAnalysis.ID, resource = Enums.Resource.CLINICAL, type = Tool.Type.OPERATION, description = "Index RGA study.")
public class RgaAnalysis extends OperationTool {
    public final static String ID = "rga-index";
    public final static String DESCRIPTION = "Generate Recessive Gene Analysis secondary index";

    private String study;
    private String file;

    private RgaManager rgaManager;

    @Override
    protected void check() throws Exception {
        super.check();

        this.rgaManager = new RgaManager(configuration, storageConfiguration);
        study = getStudyFqn();
        file = params.getString(RgaAnalysisParams.FILE);
    }

    @Override
    protected void run() throws Exception {
        step(() -> this.rgaManager.index(study, file, token));
    }

}
