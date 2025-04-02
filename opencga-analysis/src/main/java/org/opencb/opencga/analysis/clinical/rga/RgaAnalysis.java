package org.opencb.opencga.analysis.clinical.rga;

import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.analysis.rga.RgaManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = ParamConstants.ID, resource = Enums.Resource.RGA, type = Tool.Type.OPERATION, description = "Index RGA study.",
        priority = Enums.Priority.HIGH)
public class RgaAnalysis extends OperationTool {

    private String study;
    private String file;

    private RgaManager rgaManager;

    @Override
    protected void check() throws Exception {
        super.check();

        this.rgaManager = new RgaManager(getCatalogManager(), getVariantStorageManager());
        study = getStudyFqn();
        file = params.getString(ParamConstants.FILE);
    }

    @Override
    protected void run() throws Exception {
        step(() -> this.rgaManager.index(study, file, token));
    }

}
