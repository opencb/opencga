package org.opencb.opencga.core.config;

public class OperationConfig {

    private OperationExecutionConfig variantSecondaryAnnotationIndex;
    private OperationExecutionConfig variantSecondarySampleIndex;

    public OperationConfig() {
        variantSecondaryAnnotationIndex = new OperationExecutionConfig();
        variantSecondarySampleIndex = new OperationExecutionConfig();
    }

    public OperationConfig(OperationExecutionConfig variantSecondaryAnnotationIndex, OperationExecutionConfig variantSecondarySampleIndex) {
        this.variantSecondaryAnnotationIndex = variantSecondaryAnnotationIndex;
        this.variantSecondarySampleIndex = variantSecondarySampleIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OperationConfig{");
        sb.append("variantSecondaryAnnotationIndex=").append(variantSecondaryAnnotationIndex);
        sb.append(", variantSecondarySampleIndex=").append(variantSecondarySampleIndex);
        sb.append('}');
        return sb.toString();
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
