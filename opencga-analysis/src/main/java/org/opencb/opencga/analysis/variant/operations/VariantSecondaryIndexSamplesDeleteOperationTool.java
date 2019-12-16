package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;

@Tool(id = VariantSecondaryIndexSamplesDeleteOperationTool.ID,
        description = VariantSecondaryIndexSamplesDeleteOperationTool.DESCRIPTION,
        type = Tool.ToolType.VARIANT)
public class VariantSecondaryIndexSamplesDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-secondary-index-samples-delete";
    public static final String DESCRIPTION = "Remove a secondary index from the search engine for a specific set of samples.";

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.removeSearchIndexSamples(getStudyFqn(), params.getAsStringList("samples"), params, token);
        });
    }
}
