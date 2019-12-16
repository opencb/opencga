package org.opencb.opencga.core.api.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

public class VariantAnnotationDeleteParams extends ToolParams {
    private String annotationId;

    public VariantAnnotationDeleteParams() {
    }

    public VariantAnnotationDeleteParams(String annotationId) {
        this.annotationId = annotationId;
    }

    public String getAnnotationId() {
        return annotationId;
    }

    public VariantAnnotationDeleteParams setAnnotationId(String annotationId) {
        this.annotationId = annotationId;
        return this;
    }
}
