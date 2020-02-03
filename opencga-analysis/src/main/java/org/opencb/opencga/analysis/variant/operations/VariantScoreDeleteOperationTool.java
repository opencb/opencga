package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantScoreDeleteParams;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id= VariantScoreDeleteOperationTool.ID,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantScoreDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-score-delete";
    private VariantScoreDeleteParams scoreDeleteParams = new VariantScoreDeleteParams();

    @Override
    protected void check() throws Exception {
        super.check();

        scoreDeleteParams.updateParams(params);
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.variantScoreDelete(getStudyFqn(), scoreDeleteParams.getScoreName(), params, token);
        });
    }
}
