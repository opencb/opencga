package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantSampleIndexParams;
import org.opencb.opencga.core.models.common.Enums;

import java.util.ArrayList;
import java.util.List;

@Tool(id = VariantSampleIndexOperationTool.ID, description = VariantSampleIndexOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantSampleIndexOperationTool extends OperationTool {

    public static final String ID = "variant-sample-index";
    public static final String DESCRIPTION = "Build and annotate the sample index";
    protected String study;
    private VariantSampleIndexParams sampleIndexParams;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();

        sampleIndexParams = VariantSampleIndexParams.fromParams(VariantSampleIndexParams.class, params);

        if (CollectionUtils.isEmpty(sampleIndexParams.getSample())) {
            throw new IllegalArgumentException("Empty list of samples");
        }
        if (!sampleIndexParams.isBuildIndex() && !sampleIndexParams.isAnnotate()) {
            sampleIndexParams.setBuildIndex(true);
            sampleIndexParams.setAnnotate(true);
        }
    }

    @Override
    protected List<String> getSteps() {
        ArrayList<String> steps = new ArrayList<>();
        if (sampleIndexParams.isBuildIndex()) {
            steps.add("buildIndex");
        } else if (sampleIndexParams.isAnnotate()) {
            steps.add("annotate");
        }
        return steps;
    }

    @Override
    protected void run() throws Exception {
        if (sampleIndexParams.isBuildIndex()) {
            step("buildIndex", () -> variantStorageManager.sampleIndex(study, sampleIndexParams.getSample(), params, token));
        }
        if (sampleIndexParams.isAnnotate()) {
            step("annotate", () -> variantStorageManager.sampleIndexAnnotate(study, sampleIndexParams.getSample(), params, token));
        }
    }
}
