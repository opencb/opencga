package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.VariantPruneParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = VariantPruneOperationTool.ID, description = VariantPruneOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantPruneOperationTool extends OperationTool {

    public static final String DESCRIPTION = "Prune orphan variants from studies in a project.";
    public static final String ID = "variant-prune";

    @ToolParams
    protected VariantPruneParams params = new VariantPruneParams();

    @Override
    protected void run() throws Exception {

        step(() -> {
            getVariantStorageManager().variantPrune(params.getProject(), getOutDir().toUri(), params, getToken());
        });


    }
}
