package org.opencb.opencga.core.models.file;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileInternalVariant {

    @DataField(description = ParamConstants.FILE_INTERNAL_VARIANT_INDEX_DESCRIPTION)
    private FileInternalVariantIndex index;
    @DataField(description = ParamConstants.FILE_INTERNAL_VARIANT_ANNOTATION_INDEX_DESCRIPTION)
    private FileInternalVariantAnnotationIndex annotationIndex;
    @DataField(description = ParamConstants.FILE_INTERNAL_VARIANT_SECONDARY_INDEX_DESCRIPTION)
    private FileInternalVariantSecondaryIndex secondaryIndex;

    public FileInternalVariant() {
    }

    public FileInternalVariant(FileInternalVariantIndex index, FileInternalVariantAnnotationIndex annotationIndex,
                               FileInternalVariantSecondaryIndex secondaryIndex) {
        this.index = index;
        this.annotationIndex = annotationIndex;
        this.secondaryIndex = secondaryIndex;
    }

    public static FileInternalVariant init() {
        return new FileInternalVariant(FileInternalVariantIndex.init(), FileInternalVariantAnnotationIndex.init(),
                FileInternalVariantSecondaryIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalVariant{");
        sb.append("index=").append(index);
        sb.append(", annotationIndex=").append(annotationIndex);
        sb.append(", secondaryIndex=").append(secondaryIndex);
        sb.append('}');
        return sb.toString();
    }

    public FileInternalVariantIndex getIndex() {
        return index;
    }

    public FileInternalVariant setIndex(FileInternalVariantIndex index) {
        this.index = index;
        return this;
    }

    public FileInternalVariantAnnotationIndex getAnnotationIndex() {
        return annotationIndex;
    }

    public FileInternalVariant setAnnotationIndex(FileInternalVariantAnnotationIndex annotationIndex) {
        this.annotationIndex = annotationIndex;
        return this;
    }

    public FileInternalVariantSecondaryIndex getSecondaryIndex() {
        return secondaryIndex;
    }

    public FileInternalVariant setSecondaryIndex(FileInternalVariantSecondaryIndex secondaryIndex) {
        this.secondaryIndex = secondaryIndex;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalVariant that = (FileInternalVariant) o;
        return Objects.equals(index, that.index) &&
                Objects.equals(annotationIndex, that.annotationIndex) &&
                Objects.equals(secondaryIndex, that.secondaryIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, annotationIndex, secondaryIndex);
    }
}
