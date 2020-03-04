package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.common.Status;

public class SampleInternal {

    private Status status;

    public SampleInternal() {
    }

    public SampleInternal(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public SampleInternal setStatus(Status status) {
        this.status = status;
        return this;
    }
}
