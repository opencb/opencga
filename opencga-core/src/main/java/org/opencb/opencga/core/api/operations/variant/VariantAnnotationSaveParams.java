package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantAnnotationSaveParams extends ToolParams {
    public static final String DESCRIPTION = "Variant annotation save params";
    private String annotationId;

    public VariantAnnotationSaveParams() {
    }

    public VariantAnnotationSaveParams(String annotationId) {
        this.annotationId = annotationId;
    }

    public String getAnnotationId() {
        return annotationId;
    }

    public VariantAnnotationSaveParams setAnnotationId(String annotationId) {
        this.annotationId = annotationId;
        return this;
    }
}
