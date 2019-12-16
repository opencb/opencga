package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.operations.variant.VariantAnnotationDeleteParams;

@Tool(id = VariantAnnotationDeleteOperationTool.ID, description = VariantAnnotationDeleteOperationTool.ID, type = Tool.ToolType.VARIANT)
public class VariantAnnotationDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-annotation-delete";
    public static final String DESCRIPTION = "Deletes a saved copy of variant annotation";
    private VariantAnnotationDeleteParams variantAnnotationDeleteParams;
    private String project;

    @Override
    protected void check() throws Exception {
        super.check();

        variantAnnotationDeleteParams = VariantAnnotationDeleteParams.fromParams(VariantAnnotationDeleteParams.class, params);
        project = getProjectFqn();

    }

    @Override
    protected void run() throws Exception {
        step(()->{
            variantStorageManager.deleteAnnotation(project, variantAnnotationDeleteParams.getAnnotationId(), params, token);
        });
    }
}
