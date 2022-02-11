package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.IndexStatus;

import java.util.Objects;

public class SampleInternalVariantGenotypeIndex {

    private IndexStatus status;

    public SampleInternalVariantGenotypeIndex() {
    }

    public SampleInternalVariantGenotypeIndex(IndexStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantGenotypeIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public IndexStatus getStatus() {
        return status;
    }

    public SampleInternalVariantGenotypeIndex setStatus(IndexStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleInternalVariantGenotypeIndex that = (SampleInternalVariantGenotypeIndex) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }
}
