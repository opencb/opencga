package org.opencb.opencga.core.models.sample;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

public class SampleInternalVariantSecondaryAnnotationIndex {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private IndexStatus status;

    public SampleInternalVariantSecondaryAnnotationIndex() {
    }

    public SampleInternalVariantSecondaryAnnotationIndex(IndexStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantSecondaryIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public IndexStatus getStatus() {
        return status;
    }

    public SampleInternalVariantSecondaryAnnotationIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleInternalVariantSecondaryAnnotationIndex that = (SampleInternalVariantSecondaryAnnotationIndex) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
