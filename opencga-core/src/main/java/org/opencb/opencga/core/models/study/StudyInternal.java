package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.common.Status;

public class StudyInternal {

    private Status status;

    public StudyInternal() {
    }

    public StudyInternal(Status status) {
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

    public StudyInternal setStatus(Status status) {
        this.status = status;
        return this;
    }
}
