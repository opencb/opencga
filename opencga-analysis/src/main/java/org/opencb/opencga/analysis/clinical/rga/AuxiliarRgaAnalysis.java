package org.opencb.opencga.analysis.clinical.rga;

import org.opencb.opencga.analysis.rga.RgaManager;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = AuxiliarRgaAnalysis.ID, resource = Enums.Resource.CLINICAL, type = Tool.Type.OPERATION,
        description = AuxiliarRgaAnalysis.DESCRIPTION)
public class AuxiliarRgaAnalysis extends OperationTool {
    public final static String ID = "rga-aux-index";
    public final static String DESCRIPTION = ParamConstants.INDEX_AUXILIAR_COLLECTION_DESCRIPTION;

    private String study;

    private RgaManager rgaManager;

    @Override
    protected void check() throws Exception {
        super.check();

        this.rgaManager = new RgaManager(configuration, storageConfiguration);
        study = getStudyFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> this.rgaManager.generateAuxiliarCollection(study, token));
    }

}
