package org.opencb.opencga.core.models.file;

public class FileInternalVariant {

    private FileInternalVariantIndex index;
    private FileInternalVariantAnnotationIndex annotationIndex;
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
}
