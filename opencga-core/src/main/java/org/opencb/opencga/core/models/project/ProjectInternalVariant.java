package org.opencb.opencga.core.models.project;

import org.opencb.opencga.core.models.variant.InternalVariantOperationIndex;

public class ProjectInternalVariant {

    private InternalVariantOperationIndex annotationIndex;
    private InternalVariantOperationIndex secondaryAnnotationIndex;

    public ProjectInternalVariant() {
        this(new InternalVariantOperationIndex(), new InternalVariantOperationIndex());
    }

    public ProjectInternalVariant(InternalVariantOperationIndex annotationIndex, InternalVariantOperationIndex secondaryAnnotationIndex) {
        this.annotationIndex = annotationIndex;
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternalVariant{");
        sb.append("annotationIndex=").append(annotationIndex);
        sb.append(", secondaryAnnotationIndex=").append(secondaryAnnotationIndex);
        sb.append('}');
        return sb.toString();
    }

    public InternalVariantOperationIndex getAnnotationIndex() {
        return annotationIndex;
    }

    public ProjectInternalVariant setAnnotationIndex(InternalVariantOperationIndex annotationIndex) {
        this.annotationIndex = annotationIndex;
        return this;
    }

    public InternalVariantOperationIndex getSecondaryAnnotationIndex() {
        return secondaryAnnotationIndex;
    }

    public ProjectInternalVariant setSecondaryAnnotationIndex(InternalVariantOperationIndex secondaryAnnotationIndex) {
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
        return this;
    }
}
