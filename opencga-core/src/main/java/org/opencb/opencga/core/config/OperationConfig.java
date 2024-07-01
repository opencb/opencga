package org.opencb.opencga.core.config;

public class OperationConfig {

    private OperationExecutionConfig annotationIndex;
    private OperationExecutionConfig variantSecondaryAnnotationIndex;
    private OperationExecutionConfig variantSecondarySampleIndex;

    public OperationConfig() {
        this.annotationIndex = new OperationExecutionConfig();
        this.variantSecondaryAnnotationIndex = new OperationExecutionConfig();
        this.variantSecondarySampleIndex = new OperationExecutionConfig();
    }

    public OperationConfig(OperationExecutionConfig annotationIndex, OperationExecutionConfig variantSecondaryAnnotationIndex,
                           OperationExecutionConfig variantSecondarySampleIndex) {
        this.annotationIndex = annotationIndex;
        this.variantSecondaryAnnotationIndex = variantSecondaryAnnotationIndex;
        this.variantSecondarySampleIndex = variantSecondarySampleIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OperationConfig{");
        sb.append("annotationIndex=").append(annotationIndex);
        sb.append(", variantSecondaryAnnotationIndex=").append(variantSecondaryAnnotationIndex);
        sb.append(", variantSecondarySampleIndex=").append(variantSecondarySampleIndex);
        sb.append('}');
        return sb.toString();
    }

    public OperationExecutionConfig getAnnotationIndex() {
        return annotationIndex;
    }

    public OperationConfig setAnnotationIndex(OperationExecutionConfig annotationIndex) {
        this.annotationIndex = annotationIndex;
        return this;
    }

    public OperationExecutionConfig getVariantSecondaryAnnotationIndex() {
        return variantSecondaryAnnotationIndex;
    }

    public OperationConfig setVariantSecondaryAnnotationIndex(OperationExecutionConfig variantSecondaryAnnotationIndex) {
        this.variantSecondaryAnnotationIndex = variantSecondaryAnnotationIndex;
        return this;
    }

    public OperationExecutionConfig getVariantSecondarySampleIndex() {
        return variantSecondarySampleIndex;
    }

    public OperationConfig setVariantSecondarySampleIndex(OperationExecutionConfig variantSecondarySampleIndex) {
        this.variantSecondarySampleIndex = variantSecondarySampleIndex;
        return this;
    }
}
