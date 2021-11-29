package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.InternalStatus;

public class SampleInternalVariantSecondaryIndex {

    private InternalStatus status;

    public SampleInternalVariantSecondaryIndex() {
    }

    public SampleInternalVariantSecondaryIndex(InternalStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternalVariantSecondaryIndex{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public SampleInternalVariantSecondaryIndex setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }
}
