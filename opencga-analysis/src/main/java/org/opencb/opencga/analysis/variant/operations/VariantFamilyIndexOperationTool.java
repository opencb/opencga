package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.operations.variant.VariantFamilyIndexParams;

@Tool(id = VariantFamilyIndexOperationTool.ID, description = VariantFamilyIndexOperationTool.DESCRIPTION, type = Tool.ToolType.VARIANT)
public class VariantFamilyIndexOperationTool extends OperationTool {

    public static final String ID = "variant-family-index";
    public static final String DESCRIPTION = "Build the family index";

    private String study;
    private VariantFamilyIndexParams variantFamilyIndexParams;

    @Override
    protected void check() throws Exception {
        super.check();

        variantFamilyIndexParams = VariantFamilyIndexParams.fromParams(VariantFamilyIndexParams.class, params);
        study = getStudyFqn();

        if (CollectionUtils.isEmpty(variantFamilyIndexParams.getFamily())) {
            throw new IllegalArgumentException("Empty list of families");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.familyIndex(
                    study,
                    variantFamilyIndexParams.getFamily(),
                    variantFamilyIndexParams.isSkipIncompleteFamilies(),
                    params,
                    token);
        });
    }
}
