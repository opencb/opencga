package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationSaveParams;
import org.opencb.opencga.core.models.common.Enums;

@Tool(id = VariantAnnotationSaveOperationTool.ID, description = VariantAnnotationSaveOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        resource = Enums.Resource.VARIANT)
public class VariantAnnotationSaveOperationTool extends OperationTool {

    public static final String ID = "variant-annotation-save";
    public static final String DESCRIPTION = "Save a copy of the current variant annotation at the database";
    public String project;
    private VariantAnnotationSaveParams annotationSaveParams;

    @Override
    protected void check() throws Exception {
        super.check();
        annotationSaveParams = VariantAnnotationSaveParams.fromParams(VariantAnnotationSaveParams.class, params);
        project = getProjectFqn();
    }

    @Override
    protected void run() throws Exception {
        step(()->{
            variantStorageManager.saveAnnotation(project, annotationSaveParams.getAnnotationId(), params, token);
        });
    }
}
