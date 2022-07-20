package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileInternalVariantSecondaryIndex {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private IndexStatus status;

    public FileInternalVariantSecondaryIndex() {
    }

    public FileInternalVariantSecondaryIndex(IndexStatus status) {
        this.status = status;
    }

    public static FileInternalVariantSecondaryIndex init() {
        return new FileInternalVariantSecondaryIndex(new IndexStatus());
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

    public FileInternalVariantSecondaryIndex setStatus(IndexStatus status) {
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
