package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.InternalStatus;

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
}
