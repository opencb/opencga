package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.common.Status;

public class InternalStudy {

    private Status status;

    public InternalStudy() {
    }

    public InternalStudy(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InternalStudy{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public InternalStudy setStatus(Status status) {
        this.status = status;
        return this;
    }
}
