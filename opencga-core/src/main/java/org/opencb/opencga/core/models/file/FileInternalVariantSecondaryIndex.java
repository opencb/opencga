package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Objects;

public class FileInternalVariantSecondaryIndex {

    private InternalStatus status;

    public FileInternalVariantSecondaryIndex() {
    }

    public FileInternalVariantSecondaryIndex(InternalStatus status) {
        this.status = status;
    }

    public static FileInternalVariantSecondaryIndex init() {
        return new FileInternalVariantSecondaryIndex(null);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalVariantSecondaryIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public FileInternalVariantSecondaryIndex setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalVariantSecondaryIndex that = (FileInternalVariantSecondaryIndex) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
