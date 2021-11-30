package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Objects;

public class FileInternalVariantAnnotationIndex {

    private InternalStatus status;

    public FileInternalVariantAnnotationIndex() {
    }

    public FileInternalVariantAnnotationIndex(InternalStatus status) {
        this.status = status;
    }

    public static FileInternalVariantAnnotationIndex init() {
        return new FileInternalVariantAnnotationIndex(null);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalVariantAnnotationIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public FileInternalVariantAnnotationIndex setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalVariantAnnotationIndex that = (FileInternalVariantAnnotationIndex) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
