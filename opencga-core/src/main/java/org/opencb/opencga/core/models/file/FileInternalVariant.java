package org.opencb.opencga.core.models.file;

import java.util.Objects;

public class FileInternalVariant {

    private FileInternalVariantIndex index;
    private FileInternalVariantAnnotationIndex annotationIndex;
    @Deprecated
    private FileInternalVariantSecondaryAnnotationIndex secondaryIndex;
    private FileInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex;

    public FileInternalVariant() {
    }

    public FileInternalVariant(FileInternalVariantIndex index, FileInternalVariantAnnotationIndex annotationIndex,
                               FileInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex) {
        this.index = index;
        this.annotationIndex = annotationIndex;
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
    }

    public static FileInternalVariant init() {
        return new FileInternalVariant(FileInternalVariantIndex.init(), FileInternalVariantAnnotationIndex.init(),
                FileInternalVariantSecondaryAnnotationIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalVariant{");
        sb.append("index=").append(index);
        sb.append(", annotationIndex=").append(annotationIndex);
        sb.append(", secondaryIndex=").append(secondaryAnnotationIndex);
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

    @Deprecated
    public FileInternalVariantSecondaryAnnotationIndex getSecondaryIndex() {
        return secondaryIndex;
    }

    @Deprecated
    public FileInternalVariant setSecondaryIndex(FileInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex) {
        this.secondaryIndex = secondaryAnnotationIndex;
        return this;
    }

    public FileInternalVariantSecondaryAnnotationIndex getSecondaryAnnotationIndex() {
        if (secondaryAnnotationIndex == null && secondaryIndex != null) {
            return secondaryIndex;
        } else {
            return secondaryAnnotationIndex;
        }
    }

    public FileInternalVariant setSecondaryAnnotationIndex(FileInternalVariantSecondaryAnnotationIndex secondaryAnnotationIndex) {
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalVariant that = (FileInternalVariant) o;
        return Objects.equals(index, that.index) &&
                Objects.equals(annotationIndex, that.annotationIndex) &&
                Objects.equals(secondaryAnnotationIndex, that.secondaryAnnotationIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, annotationIndex, secondaryAnnotationIndex);
    }
}
