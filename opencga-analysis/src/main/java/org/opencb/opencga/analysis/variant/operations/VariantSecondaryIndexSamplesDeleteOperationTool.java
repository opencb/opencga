package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantSecondaryIndexSamplesDeleteOperationTool.ID,
        description = VariantSecondaryIndexSamplesDeleteOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        resource = Enums.Resource.VARIANT)
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
