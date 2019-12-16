package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.operations.variant.VariantAggregateParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

@Tool(id = VariantAggregateOperationTool.ID, description = VariantAggregateOperationTool.DESCRIPTION, type = Tool.ToolType.VARIANT)
public class VariantAggregateOperationTool extends OperationTool {

    public static final String ID = "variant-aggregate";
    public static final String DESCRIPTION = "Find variants where not all the samples are present, and fill the empty values,"
            + " excluding HOM-REF (0/0) values.";
    private String study;
    private VariantAggregateParams variantAggregateParams;

    @Override
    protected void check() throws Exception {
        super.check();

        variantAggregateParams = VariantAggregateParams.fromParams(VariantAggregateParams.class, params);
        params.put(VariantStorageOptions.RESUME.key(), variantAggregateParams.isResume());
        study = getStudyFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.aggregate(study, variantAggregateParams.isOverwrite(), params, token);
        });
    }
}
