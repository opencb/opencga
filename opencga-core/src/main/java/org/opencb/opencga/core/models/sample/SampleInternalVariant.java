package org.opencb.opencga.core.models.sample;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

import java.util.Objects;

public class SampleInternalVariant {

    @DataField(description = ParamConstants.SAMPLE_INTERNAL_VARIANT_INDEX_DESCRIPTION)
    private SampleInternalVariantIndex index;

    @DataField(description = ParamConstants.SAMPLE_INTERNAL_VARIANT_SAMPLE_GENOTYPE_INDEX_DESCRIPTION)
    @Deprecated
    private SampleInternalVariantSecondarySampleIndex sampleGenotypeIndex;

    @DataField(description = ParamConstants.SAMPLE_INTERNAL_VARIANT_SECONDARY_SAMPLE_INDEX_DESCRIPTION)
    private SampleInternalVariantSecondarySampleIndex secondarySampleIndex;

    @DataField(description = ParamConstants.SAMPLE_INTERNAL_VARIANT_ANNOTATION_INDEX_DESCRIPTION)
    private SampleInternalVariantAnnotationIndex annotationIndex;
    private SampleInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex;


    public SampleInternalVariant() {
    }

    public SampleInternalVariant(SampleInternalVariantIndex index, SampleInternalVariantSecondarySampleIndex secondarySampleIndex,
                                 SampleInternalVariantAnnotationIndex annotationIndex, SampleInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex) {
        this.index = index;
        this.secondarySampleIndex = secondarySampleIndex;
        this.annotationIndex = annotationIndex;
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariant{");
        sb.append("index=").append(index);
        sb.append(", secondarySampleIndex=").append(secondarySampleIndex);
        sb.append(", annotationIndex=").append(annotationIndex);
        sb.append(", secondaryAnnotationIndex=").append(secondaryAnnotationIndex);
        sb.append('}');
        return sb.toString();
    }

    public static SampleInternalVariant init() {
        return new SampleInternalVariant(null, null, null, null);
    }

    public SampleInternalVariantIndex getIndex() {
        return index;
    }

    public SampleInternalVariant setIndex(SampleInternalVariantIndex index) {
        this.index = index;
        return this;
    }

    @Deprecated
    public SampleInternalVariantSecondarySampleIndex getSampleGenotypeIndex() {
        return sampleGenotypeIndex;
    }

    @Deprecated
    public SampleInternalVariant setSampleGenotypeIndex(SampleInternalVariantSecondarySampleIndex secondarySampleIndex) {
        this.sampleGenotypeIndex = sampleGenotypeIndex;
        return this;
    }

    public SampleInternalVariantSecondarySampleIndex getSecondarySampleIndex() {
        if (secondarySampleIndex == null && sampleGenotypeIndex != null) {
            return sampleGenotypeIndex;
        }
        return secondarySampleIndex;
    }

    public SampleInternalVariant setSecondarySampleIndex(SampleInternalVariantSecondarySampleIndex secondarySampleIndex) {
        this.secondarySampleIndex = secondarySampleIndex;
        return this;
    }

    public SampleInternalVariantAnnotationIndex getAnnotationIndex() {
        return annotationIndex;
    }

    public SampleInternalVariant setAnnotationIndex(SampleInternalVariantAnnotationIndex annotationIndex) {
        this.annotationIndex = annotationIndex;
        return this;
    }

    public SampleInternalVariantSecondaryAnnotationIndex getSecondaryAnnotationIndex() {
        return secondaryAnnotationIndex;
    }

    public SampleInternalVariant setSecondaryAnnotationIndex(SampleInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex) {
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleInternalVariant that = (SampleInternalVariant) o;
        return Objects.equals(index, that.index) &&
                Objects.equals(secondarySampleIndex, that.secondarySampleIndex) &&
                Objects.equals(annotationIndex, that.annotationIndex) &&
                Objects.equals(secondaryAnnotationIndex, that.secondaryAnnotationIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, secondarySampleIndex, annotationIndex, secondaryAnnotationIndex);
    }
}
