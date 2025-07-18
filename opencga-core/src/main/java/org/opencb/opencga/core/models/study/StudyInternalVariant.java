package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.variant.InternalVariantOperationIndex;

public class StudyInternalVariant {

    @DataField(id = "secondarySampleIndex", uncommentedClasses = {"InternalVariantOperationIndex"},
            description = FieldConstants.INTERNAL_VARIANT_SECONDARY_SAMPLE_INDEX)
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
