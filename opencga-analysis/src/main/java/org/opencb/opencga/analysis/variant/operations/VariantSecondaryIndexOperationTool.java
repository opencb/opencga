package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.operations.variant.VariantSecondaryIndexParams;

@Tool(id = VariantSecondaryIndexOperationTool.ID, description = VariantSecondaryIndexOperationTool.DESCRIPTION,
        type = Tool.ToolType.VARIANT)
public class VariantSecondaryIndexOperationTool extends OperationTool {

    public static final String ID = "variant-secondary-index";
    public static final String DESCRIPTION = "Creates a secondary index using a search engine. "
            + "If samples are provided, sample data will be added to the secondary index.";
    private VariantSecondaryIndexParams indexParams;
    private String projectFqn;

    @Override
    protected void check() throws Exception {
        super.check();
        indexParams = VariantSecondaryIndexParams.fromParams(VariantSecondaryIndexParams.class, params);
        projectFqn = getProjectFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> variantStorageManager.secondaryIndex(projectFqn, indexParams.getRegion(), indexParams.isOverwrite(), params, token));
    }
}
