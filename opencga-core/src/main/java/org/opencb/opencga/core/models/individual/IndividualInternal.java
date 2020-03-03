package org.opencb.opencga.core.models.individual;

import org.opencb.opencga.core.models.common.Status;

public class IndividualInternal {

    private Status status;

    public IndividualInternal() {
    }

    public IndividualInternal(Status status) {
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualInternal{");
        sb.append("status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public IndividualInternal setStatus(Status status) {
        this.status = status;
        return this;
    }
}
