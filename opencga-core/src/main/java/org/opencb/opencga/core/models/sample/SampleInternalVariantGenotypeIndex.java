package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.InternalStatus;

public class SampleInternalVariantGenotypeIndex {

    private InternalStatus status;

    public SampleInternalVariantGenotypeIndex() {
    }

    public SampleInternalVariantGenotypeIndex(InternalStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantGenotypeIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public SampleInternalVariantGenotypeIndex setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }
}
