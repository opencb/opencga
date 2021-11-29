package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.InternalStatus;

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
}
