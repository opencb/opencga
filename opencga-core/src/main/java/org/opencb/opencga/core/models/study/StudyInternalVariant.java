package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.variant.InternalVariantOperationIndex;

public class StudyInternalVariant {

    private InternalVariantOperationIndex secondarySampleIndex;

    public StudyInternalVariant() {
        this(new InternalVariantOperationIndex());
    }

    public StudyInternalVariant(InternalVariantOperationIndex secondarySampleIndex) {
        this.secondarySampleIndex = secondarySampleIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyInternalVariant{");
        sb.append("secondarySampleIndex=").append(secondarySampleIndex);
        sb.append('}');
        return sb.toString();
    }

    public InternalVariantOperationIndex getSecondarySampleIndex() {
        return secondarySampleIndex;
    }

    public StudyInternalVariant setSecondarySampleIndex(InternalVariantOperationIndex secondarySampleIndex) {
        this.secondarySampleIndex = secondarySampleIndex;
        return this;
    }
}
