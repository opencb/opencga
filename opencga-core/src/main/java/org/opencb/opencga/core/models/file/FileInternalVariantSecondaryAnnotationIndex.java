package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

public class FileInternalVariantSecondaryAnnotationIndex {

    private IndexStatus status;

    public FileInternalVariantSecondaryAnnotationIndex() {
    }

    public FileInternalVariantSecondaryAnnotationIndex(IndexStatus status) {
        this.status = status;
    }

    public static FileInternalVariantSecondaryAnnotationIndex init() {
        return new FileInternalVariantSecondaryAnnotationIndex(new IndexStatus());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalVariantSecondaryIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public IndexStatus getStatus() {
        return status;
    }

    public FileInternalVariantSecondaryAnnotationIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalVariantSecondaryAnnotationIndex that = (FileInternalVariantSecondaryAnnotationIndex) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
