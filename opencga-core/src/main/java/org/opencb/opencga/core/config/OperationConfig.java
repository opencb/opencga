package org.opencb.opencga.core.config;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class OperationConfig {

    @DataField(id = "variantAnnotationIndex", description = FieldConstants.VARIANT_ANNOTATION_INDEX_OPERATION_CONFIGURATION)
    private OperationExecutionConfig variantAnnotationIndex;

    @DataField(id = "variantSecondaryAnnotationIndex",
            description = FieldConstants.VARIANT_SECONDARY_ANNOTATION_INDEX_OPERATION_CONFIGURATION)
    private OperationExecutionConfig variantSecondaryAnnotationIndex;

    @DataField(id = "variantSecondarySampleIndex", description = FieldConstants.VARIANT_SECONDARY_SAMPLE_INDEX_OPERATION_CONFIGURATION)
    private OperationExecutionConfig variantSecondarySampleIndex;

    public OperationConfig() {
        this.variantAnnotationIndex = new OperationExecutionConfig();
        this.variantSecondaryAnnotationIndex = new OperationExecutionConfig();
        this.variantSecondarySampleIndex = new OperationExecutionConfig();
    }

    public OperationConfig(OperationExecutionConfig variantAnnotationIndex, OperationExecutionConfig variantSecondaryAnnotationIndex,
                           OperationExecutionConfig variantSecondarySampleIndex) {
        this.variantAnnotationIndex = variantAnnotationIndex;
        this.variantSecondaryAnnotationIndex = variantSecondaryAnnotationIndex;
        this.variantSecondarySampleIndex = variantSecondarySampleIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OperationConfig{");
        sb.append("variantAnnotationIndex=").append(variantAnnotationIndex);
        sb.append(", variantSecondaryAnnotationIndex=").append(variantSecondaryAnnotationIndex);
        sb.append(", variantSecondarySampleIndex=").append(variantSecondarySampleIndex);
        sb.append('}');
        return sb.toString();
    }

    public OperationExecutionConfig getVariantAnnotationIndex() {
        return variantAnnotationIndex;
    }

    public OperationConfig setVariantAnnotationIndex(OperationExecutionConfig variantAnnotationIndex) {
        this.variantAnnotationIndex = variantAnnotationIndex;
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
