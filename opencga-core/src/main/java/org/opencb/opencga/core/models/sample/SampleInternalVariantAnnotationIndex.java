package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Objects;

public class SampleInternalVariantAnnotationIndex {

    private InternalStatus status;

    public SampleInternalVariantAnnotationIndex() {
    }

    public SampleInternalVariantAnnotationIndex(InternalStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantAnnotationIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public SampleInternalVariantAnnotationIndex setStatus(InternalStatus status) {
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
