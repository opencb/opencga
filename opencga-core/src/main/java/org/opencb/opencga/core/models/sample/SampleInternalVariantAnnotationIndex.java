package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleInternalVariantAnnotationIndex {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private IndexStatus status;

    public SampleInternalVariantAnnotationIndex() {
    }

    public SampleInternalVariantAnnotationIndex(IndexStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantAnnotationIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public IndexStatus getStatus() {
        return status;
    }

    public SampleInternalVariantAnnotationIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleInternalVariantAnnotationIndex that = (SampleInternalVariantAnnotationIndex) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
