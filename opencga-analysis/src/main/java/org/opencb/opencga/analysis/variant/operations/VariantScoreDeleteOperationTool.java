package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.operations.variant.VariantScoreDeleteParams;

@Tool(id= VariantScoreDeleteOperationTool.ID, type = Tool.ToolType.VARIANT)
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
