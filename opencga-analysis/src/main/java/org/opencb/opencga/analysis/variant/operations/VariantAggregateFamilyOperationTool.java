package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;

@Tool(id = VariantAggregateFamilyOperationTool.ID, description = VariantAggregateFamilyOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        resource = Enums.Resource.VARIANT)
public class VariantAggregateFamilyOperationTool extends OperationTool {
    public static final String ID = "variant-aggregate-family";
    public static final String DESCRIPTION = "Find variants where not all the samples are present, and fill the empty values.";
    private String study;
    private VariantAggregateFamilyParams variantAggregateFamilyParams;


    @Override
    protected void check() throws Exception {
        super.check();
        variantAggregateFamilyParams = VariantAggregateFamilyParams.fromParams(VariantAggregateFamilyParams.class, params);

        List<String> samples = variantAggregateFamilyParams.getSamples();
        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Fill gaps operation requires at least two samples!");
        }

        study = getStudyFqn();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.aggregateFamily(study, variantAggregateFamilyParams.getSamples(), params, token);
        });
    }
}
