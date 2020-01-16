package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantSecondaryIndexParams;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantSecondaryIndexSamplesOperationTool.ID,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantSecondaryIndexSamplesOperationTool extends OperationTool {

    public static final String ID = "variant-search-index-samples";
    private VariantSecondaryIndexParams indexParams;
    private String studyFqn;

    @Override
    protected void check() throws Exception {
        super.check();
        indexParams = VariantSecondaryIndexParams.fromParams(VariantSecondaryIndexParams.class, params);
        studyFqn = getStudyFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> variantStorageManager.secondaryIndexSamples(studyFqn, indexParams.getSample(), params, token));
    }
}
